package SAFP::Watcher;

use warnings;
use strict;

use 5.010;

#
# PERL INCLUDES
# 
use Carp;
use Data::Dumper;
use Digest::MD5 qw(md5_hex);


#
# SAFP INCLUDES
#
use SAFP::Watcher::Cache;
use SAFP::Watcher::File;
use SAFP::Watcher::Net;


#
# GLOBALS
#


#
# IMPLEMENTATION
#

sub new {
  my $class = shift;
  my $cfg = shift;

  if( ! defined($cfg) )
  {

    croak("No configuration provided.");
    return undef;
  }

  $cfg->{type} //= "unknown";

  my $types = {
    cache => 'Cache',
    file  => 'File',
    net   => 'Net'
  };

  my $type = $types->{ lc($cfg->{type}) } // "";

  if( $type eq "" ) {
    croak("Watcher type is not supported: " . lc($cfg->{type}) . " (" . join(",", keys( %{ $types })) . ")" );
  }

  my $watcher_path = "SAFP::Watcher::$type";

  return $watcher_path->new($cfg);
}

sub handle {
  return undef;
}

sub read {
  croak("read: is a virtual stub and needs to be overwritten.");
}

sub add_reader {
  croak("add_reader: is a virtual stub and needs to be overwritten.");
}

sub add_bookmark_store {
  #croak("add_bookmark: is a virtual stub and needs to be overwritten.");
}

sub checksum {
  my $self = shift;
  my $line = shift;

  md5_hex($line); 
}

1;
