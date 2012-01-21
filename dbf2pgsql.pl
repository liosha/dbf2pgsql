#!/usr/bin/env perl

# $Id$

#
# Usage: perl dbf2pgsql.pl file.dbf > file.sql
#
# Requires DBD::XBase
#

use 5.010;
use strict;
use warnings;
use autodie;
use utf8;

use Encode;
use XBase;
use Getopt::Long;


our $VERSION = 0.01;

#### Settings

my $encoding    = 'cp866';
my $insert_size = 1000;
my $clear_mode  = 'drop'; # drop, delete

GetOptions(
    'c|encoding=s'      => \$encoding,
    's|insert_size=i'   => \$insert_size,
    'clear=s'           => \$clear_mode,
);


#### Main

my $dbf = shift @ARGV;
my $table = XBase->new( $dbf );

my $table_name = lc $dbf;
$table_name =~ s/\.dbf$//ixms;

binmode STDOUT, ':encoding(utf-8)';


# field description
my %type_desc = (
    N => {
            desc => sub {
                my ($p, $s) = @_;
                $s      ?   "NUMERIC($p,$s)" :
                $p > 18 ?   "NUMERIC($p)" :
                $p > 9  ?   "BIGINT" :
                            "INTEGER";
            },
            dont_quote => 1,
        },
    C => { desc => sub { sprintf 'VARCHAR(%d)', @_ } },
    D => { desc => sub { 'TIMESTAMP' } },
    L => { desc => sub { 'BOOLEAN' } },
    M => { desc => sub { 'TEXT' } },
);
my @fields;
for my $name ( $table->field_names() ) {
    my $type = $table->field_type($name);
    push @fields, {
        name   => $name,
        type   => $type,
        desc   => $type_desc{$type}->{desc}->( $table->field_length($name), $table->field_decimal($name) ) ,
        quote  => !$type_desc{$type}->{dont_quote},
    };
}


# dump
given ( $clear_mode ) {
    when ( 'drop' ) {
        print "DROP TABLE IF EXIST $table_name;\n\n";
        print get_create_statement();
    }
    when ( 'delete' ) {
        print "DELETE FROM $table_name;\n\n";
    }
}

my $cursor = $table->prepare_select();
my @rows;
while ( my $row = $cursor->fetch_hashref() ) {
    push @rows, $row;
    if ( @rows == $insert_size ) {
        print get_insert_statement( \@rows );
        @rows = ();
    }
}
print get_insert_statement( \@rows );


#### Subs

sub get_create_statement
{
    return "CREATE TABLE $table_name (\n"
        . join( qq{,\n}, map { qq{    ${\( lc $_->{name})} $_->{desc}} } @fields )
        . qq{\n);\n\n};
}


sub get_insert_statement
{
    my ($rows) = @_;
    return q{} if !@$rows;

    return "INSERT INTO $table_name ( "
        . join( q{, }, map {lc $_->{name}} @fields )
        . " ) VALUES\n"
        . join( qq{,\n}, map { q{    } . get_insert_row($_) } @$rows )
        . qq{;\n\n};
}


sub get_insert_row
{
    my ($row) = @_;
    return q{( } . join( q{, }, map { pg_quote($row->{$_->{name}}, $_) } @fields ) . q{ )};
}


INIT {
my @escape = qw/ ' \\ /;
my %escape = ( "\b" => 'b', "\f" => 'f', "\n" => 'n', "\r" => 'r', "\t" => 't', );
my $escape_re = qr/ [${\( join q{}, ( @escape, keys %escape ) )}] /xms;


sub pg_quote
{
    my ($text, $row_info) = @_;
    return 'NULL' if !defined $text;
    return $text  if !$row_info->{quote};

    $text = decode $encoding, $text  if $encoding;
    my $is_escaped = $text =~ s#($escape_re)#q{\\}.($escape{$1}//$1)#egxms;
    return ( $is_escaped ? 'E' : q{} ) . qq{'$text'};    
}
}



