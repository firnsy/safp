package SAFP::Watcher::File;

use warnings;
use strict;

use 5.010;

use base qw(SAFP::Watcher);

#
# PERL INCLUDES
#
use AnyEvent;
use AnyEvent::Handle;
use Carp;
use Data::Dumper;
use Fcntl qw(SEEK_SET SEEK_CUR SEEK_END O_RDONLY O_WRONLY O_CREAT O_EXCL O_TRUNC);
use JSON;
use Scalar::Util qw(weaken);


#
# GLOBALS
#


#
# IMPLEMENTATION
#

sub new {
  my $class = shift;
  my $cfg = shift;

  my $self = bless({
    _type           => undef,
    _cfg            => $cfg // {},
    _reader         => undef,
    _read_interval  => 5,
    _watch_interval => 5,
    _readers        => [],
  }, $class);

  $self->_setup(),

  return $self;
}

sub add_reader {
  my ($self, $cb) = @_;

  croak("Callback is not defined or a code references.") if( ref($cb) ne 'CODE' );

  push( @{ $self->{_readers} }, $cb );
}

sub add_bookmark_store {
  my $self = shift;
  my $bookmark_store = shift;

  $self->{_bookmark_store} = $bookmark_store;
}

sub bookmark {
  my $self = shift;

  my $offset = 0;

  if( defined($self->{_rbuf}) &&
      defined($self->{_offset}) ) {
    $self->{_bookmark_store}{ $self->{_url} } = {
      path   => $self->{_path},
      offset => $self->{_offset} - length($self->{_rbuf}),
      csum   => ''
    };
  }
}

#
# PRIVATE USAGE
#

sub _setup {
  my $self = shift;

  my $cfg = $self->{_cfg};

  # sanity check on required config parameters
  if( ! ( defined($cfg->{dir}) && defined($cfg->{file}) ) ) {
    croak("No directory configured for file watcher.");
  }
  elsif( ! $cfg->{dir} =~ /^\./ ) {
    croak("Absolute dirs are required.");
  }
  elsif( ! -d -r $cfg->{dir} ) {
    croak("File directory doesn't exist or is not readable." . $cfg->{dir});
  }

  # add unique-ness if no prefix is specified
  $cfg->{prefix} //= `hostname | xargs echo -n`;

  # cleanup trailing and leading slashes
  $cfg->{dir}  =~ s/\/$//;
  $cfg->{file} =~ s/^\///;

  $self->{_url} = 'file://' . $cfg->{prefix} . $cfg->{dir} . '/' . $cfg->{file};

  # establish our JSON codec
  $self->{_json} = JSON->new->utf8;

  my $protocols = {
    cache => 'json',
    json  => 'json',
    line  => 'line',  # assumed default
  };

  $cfg->{proto} //= ($cfg->{type} eq 'cache' ? 'cache' : 'line');
  $self->{_proto} = $protocols->{ $cfg->{proto} };

  # weaken our self for use in callbacks
  weaken($self);

  if( $self->{_proto} eq 'cache' ) {

    $self->{decoder} = sub {
      my $ref = eval {
        $self->{_json}->incr_parse($self->{_rbuf})
      };

      if( $ref ) {
        $self->{_rbuf} = $self->{_json}->incr_text;
        $self->{_json}->incr_text = '';

        return $ref;
      }
      elsif( $@ ) {
        $self->{_json}->incr_skip;
        $self->{_rbuf} = $self->{_json}->incr_text;

        say("JSON: DECODE POOPED!");
      }
      else {
        $self->{_rbuf} = '';
      }

      return undef;
    };
  }
  else {
    $self->{decoder} = sub {
      # throws warning
      $self->{_rbuf} =~ s/^([^\015\012]*)(\015?\012)// or return undef;

      return {
        safp => '0.1',
        rcpt => $self->{_url},
        data => $1,
        dlen => length($1),
        csum => $self->checksum($1)
      };
    };
  }

  $self->_open_with_bookmark();
}

sub _open_with_bookmark {
  my $self = shift;

  return if( $self->{_path_h} );

  my $bookmark = $self->{_bookmark_store}{ $self->{_url} } // {};

  # get list of available files
  my $glob_list = $self->{_cfg}{dir} . '/' . $self->{_cfg}{file};
  $self->{_path_list} = [ < $glob_list > ];

  $self->{_offset} //= $bookmark->{offset} // 0;
  $self->{_path}   //= $bookmark->{path}   // '';
  my $csum = $bookmark->{csum} // '';

  # clear bookmark if no files available and try again later
  if( ! @{ $self->{_path_list} } ) {
    $bookmark->{path} = '';
    $bookmark->{offset} = 0;

    return $self->_schedule_open();
  }

  # set path to first in list if bookmark path is not available
  if( ! grep { $self->{_path} eq $_ } @{ $self->{_path_list} } ) {
    $self->{_path} = $self->{_path_list}->[0];
  }

  if ( ! -f $self->{_path} ) {
    return $self->_schedule_open();
  }

  croak("Unable to open path: " . $self->{_path}) if( ! (sysopen( $self->{_path_h}, $self->{_path}, O_RDONLY) ) );

  undef( $self->{_timer_watch} );

  say("  - Watching on file:/" . $self->{_path});
  AnyEvent::Util::fh_nonblocking $self->{_path_h}, 1;

  sysseek($self->{_path_h}, $self->{_offset}, SEEK_SET);

  $self->{_rbuf} = '';

  {
    weaken($self);

    $self->{_timer_read} = AE::timer 0, $self->{_read_interval}, sub {
      $self->_read_rbuf();
    }
  }
}

sub _close {
  my $self = shift;

  return if ( ! $self->{_path_h} );

  close( $self->{_path_h} );
  undef( $self->{_path_h} );

  undef( $self->{_timer_read} );
}

sub _schedule_open {
  my $self = shift;

  weaken($self);

  $self->{_timer_watch} = AE::timer 0, $self->{_watch_interval}, sub {
    $self->_open_with_bookmark();
  };
}

sub _read_rbuf {
  my $self = shift;

  my $len = sysread($self->{_path_h}, $self->{_rbuf}, 8192, length($self->{_rbuf}));

  if( $len > 0 ) {
    $self->_drain_rbuf();
    $self->{_offset} += $len;
  }
  elsif( defined($len) ) {
    $self->_drain_rbuf();

    my $eof = syseof($self->{_path_h}) + 0;
    my $bookmark = $self->{_bookmark_store}{ $self->{_url} };

    # check if file has shrunk with respect to our expected offset
    if( $self->{_offset} > $eof ) {

      # assume the file has rolled, so start at the beginning
      $self->{_offset} = 0;
      sysseek($self->{_path_h}, $self->{_offset}, SEEK_SET);
    }
    # check if our path is still valid
    elsif ( ! -f $self->{_path} ) {
      $self->_close();

      $self->{_path} = '';
      $self->{_offset} = 0;

      $self->_open_with_bookmark();
    }
    # check to see if we have successor files
    else {
      # get list of available files
      my $glob_list = $self->{_cfg}{dir} . '/' . $self->{_cfg}{file};
      $self->{_path_list} = [ < $glob_list > ];
      my $path_count = @{ $self->{_path_list} };

      my ($index) = grep { $self->{_path_list}->[$_] eq $self->{_path} } (0..$path_count-1);

      # open successor file if exists
      if( defined($index) && $index < $path_count-1 ) {
        $self->_close();

        # update the bookmark with next file details
        $self->{_path} = $self->{_path_list}[$index + 1];
        $self->{_offset} = 0;

        $self->_open_with_bookmark();
      }
    }
  }
  else{
    say("ERROR: I SUCK");
  }
}

sub _drain_rbuf {
  my $self = shift;

  return if( $self->{_skip_drain_buf} );
  local $self->{_skip_drain_buf} = 1;

  while () {

    my $len = length( $self->{_rbuf} );

    last unless $len;

    $self->_on_read($self->{_rbuf});

    if( $len == length( $self->{_rbuf} ) ) {
      say("ARE YOU GOING TO CONSUME OR NOT!!!");
      last;
    }
  }
}

sub _on_read {
  my $self = shift;

  my $safp_pack = $self->{decoder}->($self->{_rbuf});

  return if( ! defined($safp_pack) );

  foreach my $r ( @{ $self->{_readers} } ) {
    $r->( $self, $safp_pack );
  }
}

sub systell {
  sysseek($_[0], 0, SEEK_CUR);
}

sub syseof {
  my $offset = systell($_[0]);
  sysseek($_[0], 0, SEEK_END);
  my $eof = systell($_[0]);
  sysseek($_[0], $offset, SEEK_SET);
  return $eof;
}

1;
