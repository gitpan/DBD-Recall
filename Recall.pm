# -*-cperl-*-
#
# DBD::Recall - Database fault tolerance through replication.
# Copyright (c) 2000 Ashish Gulhati <hash@netropolis.org>
#
# All rights reserved. This code is free software; you can
# redistribute it and/or modify it under the same terms as Perl
# itself.
#
# $Id: Recall.pm,v 1.7 2000/11/14 21:26:13 cvs Exp $

use 5.005;
use DBI;
use Carp;
use Data::Dumper;
use Replication::Recall::Client;

package DBD::Recall;

use strict;
use vars qw($err $errstr $sqlstate $drh $VERSION $AUTOLOAD);

( $VERSION ) = '$Revision: 1.7 $' =~ /\s+([\d\.]+)/;

$err = 0;		# holds error code   for DBI::err
$errstr = "";		# holds error string for DBI::errstr
$sqlstate = "";         # holds error state  for DBI::state
$drh = undef;		# holds driver handle once initialised

my $str = Replication::Recall::Client::new_String(); my $clerk;
my $ex = Replication::Recall::Client::new_RecallException();
my $xs = defined &Data::Dumper::Dumpxs;

sub driver {
  return $drh if $drh; my $drid;
  my $class = shift; $class .= "::dr"; my $attribs = shift;
  if (defined $attribs->{Replicas}) {
    $clerk = Replication::Recall::Client::new_Clerk($attribs->{Replicas});
    return undef unless $drid = DBD::Recall::_delegate('', 'Top', 'driver');
  }
  $drh = DBI::_new_drh
    ($class, 
     {'Name'         => 'Recall',
      'Version'      => $VERSION,
      'Err'          => \$DBD::Recall::err,
      'Errstr'       => \$DBD::Recall::errstr,
      'State'        => \$DBD::Recall::state,
      'Attribution'  => 'DBD::Recall by Ashish Gulhati',
      'driver_clerk' => $clerk,
      'driver_excpn' => $ex,
      'driver_replicas'  => $attribs->{Replicas},
      'driver_drid'  => $drid,
     });
}

sub _delegate {
  my $handle = shift; my $meta = shift; my $method = shift; my $x;
  my %cmd = ('Meta'     => $meta,
	     'Method'   => $method,
	     'Args'     => \@_,
	     'Handle'   => $handle);
  do {
    Replication::Recall::Client::Clerk_read($clerk, 'LockWrite', $str, $ex);
    $x = Replication::Recall::Client::String_c_str($str)
  } while ($x ne 'Locked');
  $x = $xs?Data::Dumper::DumperX(\%cmd):Data::Dumper::Dumper(\%cmd);
  my $result = Replication::Recall::Client::Clerk_write($clerk, $x, $str, $ex);
  my $VAR1 = undef; eval Replication::Recall::Client::String_c_str($str);
  return undef unless $VAR1; my %ret = %$VAR1; 
  $Carp::CarpLevel=1; Carp::carp ($ret{Warn}) if $ret{Warn}; 
  Carp::croak ($ret{Die}) if $ret{Die}; Carp::croak ($ret{Eval}) if $ret{Eval};
  ($errstr, $err, $sqlstate) = @ret{qw(Error Err State)}; 
  return undef unless my $retref = $ret{Return}; my @ret = @$retref; 
  my ($ret) = @ret unless $#ret; return ($#ret?@ret:$ret);
}  

package DBD::Recall::dr;

use vars qw($AUTOLOAD);
$DBD::Recall::dr::imp_data_size = 0;

sub connect () {
  my $self = shift; my $dsn = shift;
  my ($user, $auth, $attr) = @_;
  unless ($self->{driver_replicas}) {
    return undef unless $dsn =~ /database=([^;]+)/; my $replicas = $1;
    $clerk = Replication::Recall::Client::new_Clerk($replicas);
    return undef unless my $drid = DBD::Recall::_delegate('', 'Top', 'driver');
    $self->{driver_clerk} = $clerk; $self->{driver_replicas} = $replicas;
    $self->{driver_drid} = $drid;
  }
  return undef unless my $dbid = 
    DBD::Recall::_delegate($self->{driver_drid}, 'AutoDR', 'connect', @_);
  my $dbh = DBI::_new_dbh 
    ($self, 
     {'Name'         => $dsn,
      'USER'         => $user,
      'CURRENT_USER' => $user,
      %$attr,
      'driver_dbid'  => $dbid,
     });
}

sub disconnect_all {
  my $self = shift;
  DBD::Recall::_delegate($self->{driver_drid}, 'AutoDR', 'disconnect_all', @_);
}

sub DESTROY {
  my $self = shift;
  DBD::Recall::_delegate($self->{driver_drid}, 'AutoDR', 'DESTROY', @_);
}

sub AUTOLOAD {
  my $self = shift; (my $auto = $AUTOLOAD) =~ s/.*:://;
  DBD::Recall::_delegate($self->{driver_drid}, 'AutoDR', $auto, @_);
}

package DBD::Recall::db;

use vars qw($AUTOLOAD);
$DBD::Recall::db::imp_data_size = 0;

sub prepare {
  my $self = shift;
  my ($statement, @attribs) = @_;
  return undef unless my $stid = 
    DBD::Recall::_delegate($self->{driver_dbid}, 'AutoDB', 'prepare', @_);
  my $sth = DBI::_new_sth
    ($self,
     {'Statement'     => $statement,
      'driver_stid'   => $stid,
      'NUM_OF_PARAMS' => ($statement =~ tr/?//),
     });
}

sub disconnect {
  my $self = shift;
  DBD::Recall::_delegate($self->{driver_dbid}, 'AutoDB', 'disconnect', @_);
}

sub tables {
  my $self = shift;
  DBD::Recall::_delegate($self->{driver_dbid}, 'AutoDB', 'tables', @_);
}

sub table_info {
  my $self = shift;
  DBD::Recall::_delegate($self->{driver_dbid}, 'AutoDB', 'table_info', @_);
}

sub DESTROY {
  my $self = shift;
  DBD::Recall::_delegate($self->{driver_dbid}, 'AutoDB', 'DESTROY', @_);
}

sub STORE {
  my $self = shift;
  DBD::Recall::_delegate($self->{driver_dbid}, 'AutoDB', 'STORE', @_);
}

sub FETCH {
  my $self = shift;
  DBD::Recall::_delegate($self->{driver_dbid}, 'AutoDB', 'FETCH', @_);
}  

sub AUTOLOAD {
  my $self = shift; (my $auto = $AUTOLOAD) =~ s/.*:://;
  DBD::Recall::_delegate($self->{driver_dbid}, 'AutoDB', $auto, @_);
}

package DBD::Recall::st;

use vars qw($AUTOLOAD);
$DBD::Recall::st::imp_data_size = 0;

sub bind_param () {
  my $self = shift;
  DBD::Recall::_delegate($self->{driver_stid}, 'AutoST', 'bind_params', @_);
}

sub execute {
  my $self = shift;
  DBD::Recall::_delegate($self->{driver_stid}, 'AutoST', 'execute', @_);
}

sub fetch () {
  my $self = shift;
  DBD::Recall::_delegate($self->{driver_stid}, 'AutoST', 'fetch', @_);
}

sub fetchrow_arrayref () {
  my $self = shift;
  DBD::Recall::_delegate($self->{driver_stid}, 'AutoST', 'fetchrow_arrayref', @_);
}

sub rows {
  my $self = shift;
  DBD::Recall::_delegate($self->{driver_stid}, 'AutoST', 'rows', @_);
}

sub finish {
  my $self = shift;
  DBD::Recall::_delegate($self->{driver_stid}, 'AutoST', 'finish', @_);
}

sub FETCH () {
  my $self = shift;
  DBD::Recall::_delegate($self->{driver_stid}, 'AutoST', 'FETCH', @_);
}

sub STORE () {
  my $self = shift;
  DBD::Recall::_delegate($self->{driver_stid}, 'AutoST', 'STORE', @_);
}

sub DESTROY () {
  my $self = shift;
  DBD::Recall::_delegate($self->{driver_stid}, 'AutoST', 'DESTROY', @_);
}

sub AUTOLOAD {
  my $self = shift; (my $auto = $AUTOLOAD) =~ s/.*:://;
  DBD::Recall::_delegate($self->{driver_stid}, 'AutoST', $auto, @_);
}

=pod

=head1 NAME 

DBD::Recall - Database fault tolerance through replication.

=head1 SYNOPSIS

  use DBI;

  my $replicas = '192.168.1.1:7000,192.168.1.2:7000,192.168.1.3:7000';
  my $dsn = "DBI:Recall:database=$replicas";

  my $drh = DBI->install_driver ($driver, { Replicas => $replicas }); 
  my @dbs = $drh->func( "_ListDBs" ); 
  print (join "\n",@dbs,"\n");

  my $dbh = DBI->connect($dsn);
  my @tables = $dbh->tables();
  print (join "\n",@tables,"\n");

=head1 DESCRIPTION

This module interfaces to Recall, a data replication library written
by Eric Newton, to provide transparent fault tolerance for database
applications.

Recall is based on a data replication algorithm developed at DEC's SRC
for the Echo filesystem. It implements a fast protocol with low
network overhead and guranteed fault tolerance as long as n of 2n+1
replica nodes are up.

The DBD::Recall interface allows you to add fault tolerance to your
database applications by a trivial change in your code. Simply use
this module instead of the DBD you are currently using.

To achieve replicated functionality you'll also need to set up a few
pieces of external infrastucture, such as the replica servers, and
rsync access between replicas. This is all described in greater detail
in L<Replication::Recall::DBServer>.

=head1 WARNING

DBD::Recall is a hack that attempts to accomplish something
(fault-tolerance through replication) at the perl DBD driver level
that would be better implemented by database servers. It works, but it
is not pretty.

Some commercial servers, such as Oracle, do implement replication. If
speed and reliability are critical to your application, you will
probably be better off with one of the commercial databases that
implement replication within the database engine.

I've only tried DBD::Recall with MySQL so far on Debian GNU/Linux. If
you get it to work with another database engine or on another
operating system, please email me about your experiences so I can
include information about your platform in future releases.

=head1 BUGS

=over 2

=item *

Transparency is accomplished through a remote delegation hack which
might break under certain circumstances. If this happens to you,
please let me know.

=item *

There must be loads more. Let me know if you find some.

=back

=head1 AUTHOR

DBD::Recall is Copyright (c) 2000 Ashish Gulhati
<hash@netropolis.org>.  All Rights Reserved.

=head1 ACKNOWLEDGEMENTS

Thanks to Barkha for inspiration, laughs and all 'round good times;
and to Eric Newton, Gurusamy Sarathy, Larry Wall, Richard Stallman and
Linus Torvalds for all the great software.

=head1 LICENSE

This code is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=head1 DISCLAIMER

This is free software. If it breaks, you own both parts.

=cut

'True Value';

