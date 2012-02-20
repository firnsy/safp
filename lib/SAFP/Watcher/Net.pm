package SAFP::Watcher::Net;

use warnings;
use strict;

use 5.010;

use base qw(SAFP::Watcher);

#
# PERL INCLUDES
# 
use AnyEvent::Handle;
use AnyEvent::Socket;
use Carp;
use Data::Dumper;
use JSON;


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
    _sock    => undef,
    _clients => {},
    _readers => [],
  }, $class);

  $self->_setup(),

  return $self;
}


sub _setup {
  my $self = shift;
  my $cfg = $self->{_cfg};

  croak("No host configured for listener.") if( ! defined($cfg->{host}) );
  croak("No port configured for watcher.")  if( ! defined($cfg->{port}) );
 
  $cfg->{proto} //= "tcp";

  my $protocols = {
    tcp_raw => ["tcp", "line"],
    tcp     => ["tcp", "line"],
    tcp_chk => ["tcp", "json"],
  };

  croak("Unsupported protocol: " . $cfg->{proto} . " (" . join(",", keys(%{ $protocols })) . ")") if( ! defined($protocols->{ $cfg->{proto} }) );

  $self->{_proto} = $protocols->{$cfg->{proto}}->[0];
  $self->{_proto_handler} = $protocols->{$cfg->{proto}}->[1];

  given( $self->{_proto} )
  {
    when('tcp') {
      $self->{server} = tcp_server(
        $cfg->{host},
        $cfg->{port},
        sub {
          $self->_client_connected(@_);
        },
        sub {
          my ($fh, $this_host, $this_port) = @_;
          say("  - Watching on tcp://" . $this_host . ":" . $this_port);
        }
      );
    }
  }
}

sub _client_connected
{
  my ($self, $fh, $host, $port) = @_;

  say("Connection established with: $host:$port");
 
  $self->{_clients}{ fileno($fh) } = {
    raw => AnyEvent::Handle->new(
      fh => $fh,
      on_read  => sub { $self->_on_read(@_);  },
      on_write => sub { $self->_on_write(@_); },
      on_drain => sub { $self->_on_drain(@_); },
      on_eof   => sub { $self->_on_eof(@_);   },
      on_error => sub { $self->_on_error(@_); },
    ),
    url => $self->{_proto} . '://' . $host . ':' . $port
  };

  # read callback is ($from, $data)

  # TODO: weaken the $self reference


  if( $self->{_proto_handler} eq 'line' ) {
    $self->{_proto_handler_cb} = sub {
      my ($fh, $line, $eol) = @_;

      my $fh_n = fileno($fh->{fh});

      my $safp_pack = {
        safp => '0.1',
        rcpt => $self->{_clients}{$fh_n}{url},
        data => $line,
        dlen => length($line),
        csum => 'sdfsdfasdf'
      };

      foreach my $r ( @{ $self->{_readers} } ) {
        $r->( $safp_pack );
      }
    }
  }
  else {
    croak("No handler suitable: $self->{_proto}.");
  }
}


sub _on_drain {

}

sub _on_error {

}

sub _on_read {
  my ($self, $handle) = @_;

  $handle->push_read($self->{_proto_handler} => sub { $self->{_proto_handler_cb}->(@_); });
}

sub _on_write {

}


sub _on_eof {

}

sub add_reader {
  my ($self, $cb) = @_;

  croak("Callback is not defined or a code references.") if( ref($cb) ne 'CODE' );

  push( @{ $self->{_readers} }, $cb );
}

1;
