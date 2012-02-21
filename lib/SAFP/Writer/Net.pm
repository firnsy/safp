package SAFP::Writer::Net;

use warnings;
use strict;

use 5.010;

use base qw(SAFP::Writer);

#
# PERL INCLUDES
# 
use AnyEvent;
use AnyEvent::Handle;
use AnyEvent::Socket;
use Carp;
use Data::Dumper;
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
    _type => undef,
    _cfg  => $cfg // {},
    _sock => undef,
    _clients => {},
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
  my ($self, $data, $cb) = @_;

  croak("Unexpected data type: " . ref($data)) if( ref($data) ne 'HASH' );

  my $data_write = $self->{_encoder}->($data);

  $self->{client}->push_write( $self->{_proto_handler} => $data_write );
}

#
# PRIVATE
#

sub _setup {
  my $self = shift;
  my $cfg = $self->{_cfg};

  if( ! defined($cfg->{host}) ) {
    croak("No host configured for net writer.");
  }
  elsif( ! defined($cfg->{port}) ) {
    croak("No port configured for net writer.");
  }

  $cfg->{proto} //= "tcp";

  $self->{_json} = JSON->new->utf8;

  my $protocols = {
    tcp_raw  => ["tcp", "line"],
    tcp      => ["tcp", "line"],
    tcp_safp => ["tcp", "json"],
  };

  croak("Unsupported protocol: " . $cfg->{proto} . " (" . join(",", keys(%{ $protocols })) . ")") if( ! defined($protocols->{ $cfg->{proto} }) );

  $self->{_proto} = $protocols->{$cfg->{proto}}->[0];
  $self->{_proto_handler} = $protocols->{$cfg->{proto}}->[1];

  weaken($self);

  if( $self->{_proto_handler} eq 'line' ) {
    $self->{_encoder} = sub {
      my $data = shift;

      # return the data component of the safp struct
      return $data->{data};
    }
  }
  elsif( $self->{_proto_handler} eq 'json' ) {
    $self->{_encoder} = sub {
      my $data = shift;

      # return the entire safp struct
      return $data;
    }
  }
  else {
    croak("No handler suitable: $self->{_proto}.");
  }

  # establish cache wather
  $self->{_cache} = SAFP::Watcher::Cache->new($self->{_cache_cfg});
  $self->{_cache}->set_bookmark_id( $cfg->{host} . ':' . $cfg->{port} ); 
  $self->{_cache}->add_reader(sub{
    my ($cache, $data) = @_;
    
    $cache->stop_reading();
    $self->write($data);
  });

  $self->{_cache}->start_reading();

  $self->_connect();
}

sub _connect {
  my $self = shift;
  my $cfg = $self->{_cfg};

  given( $self->{_proto} )
  {
    when('tcp') {
      $self->{client} = AnyEvent::Handle->new(
        connect          => [$cfg->{host}, $cfg->{port}],
        on_connect       => sub { $self->_on_connect(@_); },
        on_connect_error => sub { $self->_on_connect_error(@_); },
        on_error         => sub { $self->_on_error(@_); },
        on_read          => sub { $self->_on_read(@_); },
        on_drain         => sub { $self->_on_drain(@_); },
        on_eof           => sub { $self->_on_eof(@_); },
      );
    }
  }
}

sub _reconnect {
  my $self = shift;

  return if( defined($self->{_reconnect}) );

  say("Attempting reconnect in 60 seconds");

  weaken($self);

  $self->{_reconnect} = AE::timer(60, 0, sub {
    undef($self->{_reconnect});

    $self->_connect();
  });
}

sub _on_connect
{
  my ($self, $fh, $host, $port) = @_;

  say("Connected upstream.");
}

sub _on_connect_error {
  my ($self, $handle, $message) = @_;

  say("Could not connect upstream: " . $message);
  
  $self->{_connected} = 0;

  $self->_reconnect(); 
}

sub _on_error {
  my ($self, $handle, $fatal, $message) = @_;

  say("Error with upstream: " . $message);

  $self->{_connected} = 0;

  $self->_reconnect(); 
}

sub _on_read {
  my ($self, $handle) = @_;

  $handle->push_read($self->{_proto_handler} => sub { $self->{_decoder}->(@_); });
}

sub _on_drain {
  my $self = shift;
  
  say("DRAIN");

  $self->{_cache}->bookmark();
  $self->{_cache}->start_reading();
}

sub _on_eof {

}

1;
