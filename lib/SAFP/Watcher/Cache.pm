package SAFP::Watcher::Cache;

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
    _consume        => 1,
    _drain          => 1,
    _on_drained     => undef,
    _url            => undef,
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

sub set_bookmark_id {
  my ($self, $id) = @_;

  $self->{_url} = $id;
}

sub bookmark {
  my $self = shift;

  if( defined($self->{_rbuf}) &&
      defined($self->{_offset}) ) {

    $self->{_bookmark_store}{'.cache'}{ $self->{_url} } = {
      path   => $self->{_path},
      offset => $self->{_offset} - length($self->{_rbuf}),
      csum   => ''
    };
  }
}

sub stop_reading {
  my $self = shift;

  $self->{_consume} = 0;
  $self->{_drain} = 0;
}

sub start_reading {
  my $self = shift;

  croak("No ID set yet.") if( ! defined($self->{_url}) );

  $self->{_consume} = 1;
  $self->{_drain} = 1;

  $self->_open_with_bookmark();
}


#
# PRIVATE USAGE
#

sub _setup {
  my $self = shift;

  my $cfg = $self->{_cfg};

  # sanity check on required config parameters
  if( ! defined($cfg->{dir}) ) {
    croak("No directory configured for cache watcher.");
  }
  elsif( ! $cfg->{dir} =~ /^\./ ) {
    croak("Absolute dirs are required.");
  }
  elsif( ! -d -r $cfg->{dir} ) {
    croak("Cache directory doesn't exist or is not readable: " . $cfg->{dir});
  }

  # cleanup trailing and leading slashes
  $cfg->{dir}  =~ s/\/$//;
  $cfg->{file} = 'cache.*';

  # establish our JSON codec
  $self->{_json} = JSON->new->utf8;

  # weaken our self for use in callbacks
  weaken($self);

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

sub _open_with_bookmark {
  my $self = shift;

  return if( $self->{_path_h} );

  my $bookmark = $self->{_bookmark_store}{'.cache'}{ $self->{_url} } // {};

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

  say("  - " . $self->{_url} . " Watching on file:/" . $self->{_path});
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

# consume from the cache file
sub _read_rbuf {
  my $self = shift;

  # only continue if we've been told to consume
  if( ! $self->{_consume} ) {
#    say("NOT CONSUMING ON REQUEST");
    return;
  }

  my $len = sysread($self->{_path_h}, $self->{_rbuf}, 8192, length($self->{_rbuf}));

  if( $len > 0 ) {
    $self->{_offset} += $len;
    $self->_drain_rbuf();
  }
  elsif( defined($len) ) {
    $self->_drain_rbuf();

    my $eof = syseof($self->{_path_h}) + 0;
    my $bookmark = $self->{_bookmark_store}{'.cache'}{ $self->{_url} };

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
  else {
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

    # abort if we're not allowed to drain
    if( ! $self->{_drain} ) {
#      say("NOT DRAINING ON REQUEST");
      last;
    }

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
