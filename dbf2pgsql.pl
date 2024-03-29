#!/usr/bin/env perl

# $Id$

#
# Requires DBD::XBase
#

use 5.010;
use strict;
use warnings;
use autodie;
use utf8;

use Carp;

use Getopt::Long;


our $VERSION = 0.02;

#### Settings

my %options;
my $format = 'insert';


GetOptions(
    'c|encoding=s'      => sub { $options{encoding} = $_->[1] },
    's|insert-size=i'   => sub { $options{insert_size} = $_->[1] },
    'clear=s'           => \my $clear_mode,
    'f|format=s'        => \$format,
    'o|output=s'        => \my $output_fn,
);

usage()  unless @ARGV;



#### Main action

my $output = $output_fn && $output_fn ne q{-}
    ? do { open my $fh, '>', $output_fn; $fh }
    : *STDOUT;
binmode $output, ':encoding(utf-8)';


my $filemask = shift @ARGV;
for my $file ( glob $filemask ) {
    my $dbf = Dbf2Sql->new( $file, %options );

    given ( $clear_mode ) {
        when ( 'drop' ) {
            say {$output} $dbf->get_drop_statement();
            say {$output} $dbf->get_create_statement();
        }
        when ( 'delete' ) {
            say {$output} $dbf->get_delete_statement();
        }
    }

    given ( $format ) {
        when ( 'copy' ) {
            say {$output} $dbf->get_copy_statement();
            while ( my $csv = $dbf->get_next_csv_portion() ) {
                say {$output} $csv;
            }
            say {$output} q{\.};
        }
        when ( 'insert' ) {
            while ( my $statement = $dbf->get_next_insert_statement() ) {
                say {$output} $statement;
            }
        }
    }
}

close $output;
exit;




sub usage
{
print <<"USAGE_END";

    ---| dbf2pgsql  (c) 2012 liosha, xliosha\@gmail.com

Usage:
    perl dbf2pgsql.pl [options] files*.dbf  >  file.sql

Options:
    -o  --output        output file name
    -c  --encoding      dbf codepage
    -f  --format        table populating mode (insert, copy)
    -s  --insert-size   number of rows in every insert operator
        --clear         table clear mode (drop, delete)

USAGE_END

exit;
}




# -----------------------------------
# Main converter class

package Dbf2Sql;

use Encode;
use XBase;
use Carp;

INIT {

my %defaults = (
    encoding => 'cp866',
    insert_size => 1000,
);

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
    D => { desc => sub { 'DATE' } },
    L => { desc => sub { 'BOOLEAN' } },
    M => { desc => sub { 'TEXT' } },
);


sub new
{
    my ($class, $fname, %opt) = @_;
    my $self = { %defaults, %opt };

    $self->{file_name} = $fname;
    $self->{table_name} //= lc $fname;
    $self->{table_name} =~ s/\.dbf$//ixms;

    $self->{xbase} = XBase->new( $fname )
        or croak XBase->errstr;
    $self->{cursor} = $self->{xbase}->prepare_select();
       
    for my $name ( $self->{xbase}->field_names() ) {
        my $type = $self->{xbase}->field_type($name);
        push @{ $self->{fields} }, {
            name   => $name,
            type   => $type,
            desc   => $type_desc{$type}->{desc}->( $self->{xbase}->field_length($name), $self->{xbase}->field_decimal($name) ) ,
            quote  => !$type_desc{$type}->{dont_quote},
        };
    }

    bless $self, $class;
    return $self;
}


sub get_create_statement
{
    my ($self) = @_;
    return "CREATE TABLE $self->{table_name} (\n"
        . join( qq{,\n}, map { qq{    ${\( lc $_->{name})} $_->{desc}} } @{ $self->{fields} } )
        . qq{\n);\n};
}

sub get_drop_statement
{
    my ($self) = @_;
    return "DROP TABLE IF EXISTS $self->{table_name};\n";
}

sub get_delete_statement
{
    my ($self) = @_;
    return "DELETE FROM $self->{table_name};\n";
}


sub get_copy_statement
{
    my ($self) = @_;
    my $row_list = join q{, }, map {lc $_->{name}} @{ $self->{fields} };
    return "COPY $self->{table_name} ($row_list) FROM stdin;";
}


sub _get_next_data_portion
{
    my ($self) = @_;

    my @rows;
    for ( 1 .. $self->{insert_size} // 10000 ) {
        my $row = $self->{cursor}->fetch_hashref();
        last if !$row;
        push @rows, $row;
    }

    return \@rows;
}


sub get_next_csv_portion
{
    my ($self) = @_;
    my $rows = $self->_get_next_data_portion();
    return q{} if !@$rows;

    return join qq{\n}, map { $self->_get_csv_row($_) } @$rows;
}


sub _get_csv_row
{
    my ($self, $row) = @_;
    return join qq{\t}, map { $self->_pg_csv_quote($row->{$_->{name}}, $_) } @{ $self->{fields} };
}


{
my @escape = qw/ \\ /;
my %escape = ( "\b" => 'b', "\f" => 'f', "\n" => 'n', "\r" => 'r', "\t" => 't', );
my $escape_re = qr/ [${\( join q{}, map {"\\$_"} ( @escape, values %escape ) )}] /xms;

sub _pg_csv_quote
{
    my ($self, $text, $row_info) = @_;
    return '\N' if !defined $text;
    return $text  if !$row_info->{quote};

    $text = decode $self->{encoding}, $text  if $self->{encoding};
    $text =~ s#($escape_re)#q{\\}.($escape{$1}//$1)#egxms;
    return $text;
}
}



sub get_next_insert_statement
{
    my ($self) = @_;
    my $rows = $self->_get_next_data_portion();
    return q{} if !@$rows;

    return "INSERT INTO $self->{table_name} ( "
        . join( q{, }, map {lc $_->{name}} @{ $self->{fields} } )
        . " ) VALUES\n"
        . join( qq{,\n}, map { q{    } . $self->_get_insert_row($_) } @$rows )
        . qq{;\n};
}


sub _get_insert_row
{
    my ($self, $row) = @_;
    return q{( } . join( q{, }, map { $self->_pg_quote($row->{$_->{name}}, $_) } @{ $self->{fields} } ) . q{ )};
}


{
my @escape = qw/ ' \\ /;
my %escape = ( "\b" => 'b', "\f" => 'f', "\n" => 'n', "\r" => 'r', "\t" => 't', );
my $escape_re = qr/ [${\( join q{}, map {"\\$_"} ( @escape, values %escape ) )}] /xms;

sub _pg_quote
{
    my ($self, $text, $row_info) = @_;
    return 'NULL' if !defined $text;
    return $text  if !$row_info->{quote};

    $text = decode $self->{encoding}, $text  if $self->{encoding};
    my $is_escaped = $text =~ s#($escape_re)#q{\\}.($escape{$1}//$1)#egxms;
    return ( $is_escaped ? 'E' : q{} ) . qq{'$text'};    
}
}

}
