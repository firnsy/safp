package SAFP::Writer::File;

use warnings;
use strict;

use 5.010;

use base qw(SAFP::Writer);

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
  my $cache_cfg = shift;

  my $self = bless({
    _type    => undef,
    _cfg     => $cfg // {},
    _reader  => undef,
    _readers => [],
    _cache_cfg => $cache_cfg,
  }, $class);

  $self->_setup(),

  return $self;
}

sub add_cache {
  my ($self, $cache) = @_;

  $self->{_cache} = $cache;

  $self->{_cache}->set_bookmark_id( $self->{_url} );
}

sub add_bookmark_store {
  my ($self, $store) = @_;

  $self->{_cache}->add_bookmark_store($store);
}

sub write {
  my $self = shift;
  my $data = shift;

  croak("Unexpected data tsype: " . ref($data)) if( ref($data) ne 'HASH' );

  say("WRITING");

  my $path = $data->{rcpt};

  if( ! defined($self->{_handles}{ $path }{fh}) ) {
    $self->_open_file($path);
  }
  else {
    # check for roll file on time
    $self->_roll_file_on_time($path);
  }

  my $data_write = $data->{data} . "\n";

  syswrite $self->{_handles}{ $path }{fh}, $data_write, length($data_write);

  $self->{_cache}->bookmark();
  $self->{_cache}->start_reading(); 

  # check for roll file on size
  $self->_roll_file_on_size($path);
}

#
# PRIVATE
#

sub _setup {
  my $self = shift;
  my $cfg = $self->{_cfg};

  if( ! defined($cfg->{dir}) ) {
    croak("No directory configured for file writer.");
  }
  elsif( $cfg->{dir} =~ /^\./ ) {
    croak("Absolute paths are required.");
  }
  elsif( ! -d -r $cfg->{dir} ) {
    croak("Directory doesn't exist or is not readable.");
  }

  # cleanup trailing and leading slashes 
  $cfg->{dir}  =~ s/\/$//;

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


  given( $self->{_proto} ) {
    when ('json') {
      # weaken one's self for the callback reference
      weaken($self);

      $self->{encoder} = sub {
        $self->{_json}->decode(@_);
      };
    }
    default {
      $self->{encoder} = sub {
        join('', @_);
      };
    }
  }

  # establish cache wather
  $self->{_cache} = SAFP::Watcher::Cache->new($self->{_cache_cfg});
  $self->{_cache}->set_bookmark_id( $self->{_url} ); 
  $self->{_cache}->add_reader(sub{
    my ($cache, $data) = @_;
   
    say("GOT SOMETHING");
 
    $cache->stop_reading();
    $self->write($data);
  });

  $self->{_cache}->start_reading();

  say('  - Writing at: ' . $cfg->{dir} . '/');
}



sub _open_file {
  my $self = shift;
  my $path = shift;

  return if( ! defined($path) );

  my $handle = $self->{_handles}{ $path } //= {};
  my $now = int(AE::now);

  # sanitise path if required
  $handle->{path} //= $self->{_cfg}{dir} . substr($path, rindex($path, '/')) . '.' . $now;
  say('Writing file: ' . $handle->{path});

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
