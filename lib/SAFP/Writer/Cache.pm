package SAFP::Writer::Cache;

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
use Fcntl qw(SEEK_SET SEEK_CUR SEEK_END O_RDONLY O_RDWR O_EXCL O_CREAT);
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
    _type    => undef,
    _cfg     => $cfg // {},
    _reader  => undef,
    _readers => [],
  }, $class);

  $self->_setup(),

  return $self;
}

sub add_writer {
  my ($self, $cb) = @_;

  croak("Callback is not defined or a code references.") if( ref($cb) ne 'CODE' );

  push( @{ $self->{_readers} }, $cb );
}

sub add_bookmark_store {
  my $self = shift;
  my $bookmark_store = shift;

  $self->{_bookmark_store} = $bookmark_store;
}

#
# PRIVATE USAGE
#


sub _setup {
  my $self = shift;
  my $cfg = $self->{_cfg};

  croak("No directory configured for listener.") if( ! defined($cfg->{dir}) );
 
  croak("Absolute paths are required.") if( $cfg->{dir} =~ /^\./ );

  croak("Directory does not exist or is not writable.") if( ! -d -w $cfg->{dir} );

  $self->{_url} = 'file://' . $cfg->{dir};

  $self->{_json} = JSON->new->utf8;

  my $protocols = {
    cache   => 'json',
    json    => 'json',
    line    => 'line',  # assumed default
  };

  $cfg->{proto} //= ($cfg->{type} eq 'cache' ? 'cache' : 'line');
  $self->{_proto} = $protocols->{ $cfg->{proto} };

  $cfg->{roll_time} //= 0;
  $cfg->{roll_size} //= 0;

  if( $self->{_proto} eq 'json' ) {
    # weaken one's self for the callback reference
    weaken($self);

    $self->{encoder} = sub {
      $self->{_json}->encode(@_);
    };
  }
  else {
    $self->{encoder} = sub {
      join('', @_);
    };
  }

  say("  Caching at: " . $cfg->{dir});
}


sub _bookmark {
  my $self = shift;

  my $offset = 0;

  if( defined($self->{_rbuf}) &&
      defined($self->{_offset}) ) {
    $self->{_bookmark_store}{ $self->{_url} } = {
      offset => $self->{_offset} - length($self->{_rbuf}),
      csum   => ''
    };
  }
}

sub write {
  my $self = shift;
  my $data = shift;

  croak("Unexpected data type: " . ref($data)) if( ref($data) ne 'HASH' );

  my $path = $data->{rcpt};

  # check for roll file on time
  $self->_roll_file_on_time($path);

  my $data_write = $self->{encoder}->($data) . "\n";

  print $data_write;

  syswrite $self->{_handles}{ $path }{fh}, $data_write, length($data_write);

  # check for roll file on size
  $self->_roll_file_on_size($path);
}

sub _open_file {
  my $self = shift;
  my $path = shift;

  return if( ! defined($path) );

  my $handle = $self->{_handles}{ $path } //= {};
  my $now = int(AE::now);

  # sanitise path if required
  $handle->{path} //= $self->{_cfg}{dir} . '/cache.' . $now;

  # update time always
  $handle->{time} = $now;

  croak("Unable to open file: " . $handle->{path}) if( ! sysopen($handle->{fh}, $handle->{path}, O_RDWR | O_CREAT | O_EXCL) );
}

sub _roll_file_on_time {
  my $self = shift;
  my $path = shift;

  return if( ! defined($path) );
  return if( $self->{_cfg}{roll_time} <= 0);

  my $handle = $self->{_handles}{ $path } // {};

  # ensure we have a handle
  if( defined($handle->{fh}) ) {
    my $now = int(AE::now);

    if( ($now - $handle->{time}) >= $self->{_cfg}{roll_time} ) {
      say("ROLLING ON TIME");
      $self->_close_file($path);
      $self->_open_file($path);
    }
  }
  # first time called so create file
  else {
    $self->_open_file($path);
  }
}

sub _roll_file_on_size {
  my $self = shift;
  my $path = shift;

  return if( ! defined($path) );
  return if( $self->{_cfg}{roll_size} <= 0);

  my $handle = $self->{_handles}{ $path } // {};

  # ensure we have a handle
  if( defined($handle->{fh}) ) {
    my $pos = systell($handle->{fh});

    if( defined($pos) && $pos >= $self->{_cfg}{roll_size} ) {
      say("ROLLING ON SIZE");
      $self->_close_file($path);
      $self->_open_file($path);
    }
    #
  }
  # first time called so create file
  else {
    $self->_open_file($path);
  }
}

sub _close_file {
  my $self = shift;
  my $path = shift;


  my $handle = $self->{_handles}{ $path } //= {};

  if( defined($handle->{fh}) ) {
    close($handle->{fh});

    undef($handle->{fh});
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
