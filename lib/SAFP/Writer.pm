package SAFP::Writer;

use warnings;
use strict;

use 5.010;

#
# PERL INCLUDES
# 
use Carp;
use Data::Dumper;
use Digest::MD5 qw(md5_hex);
use Scalar::Util qw(weaken);

#
# SAFP INCLUDES
#
use SAFP::Writer::Cache;
use SAFP::Writer::File;
use SAFP::Writer::Net;


#
# GLOBALS
#


#
# IMPLEMENTATION
#

sub new {
  my ($class, $cfg, $cache) = @_;

  if( ! defined($cfg) )
  {
    croak("No configuration provided.");
    return undef;
  }

  $cfg->{type} //= "unknown";

  my $types = {
    file  => 'File',
    cache => 'Cache',
    net   => 'Net',
  };

  my $type = $types->{ lc($cfg->{type}) } // "";

  if( $type eq "" ) {
    croak("Writer type is not supported: " . lc($cfg->{type}) . " (" . join(",", keys( %{ $types })) . ")" );
  }

  my $writer_path = "SAFP::Writer::$type";

  return $writer_path->new($cfg, $cache);
}

sub write {
  croak("write: is a virtual stub and needs to be overwritten.");
}

sub add_writer {
  croak("add_writer: is a virtual stub and needs to be overwritten.");
}

sub add_bookmark_store {
}

sub checksum {
  my $self = shift;
  my $line = shift;

  md5_hex($line); 
}

1;
