use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile (
	       'NAME'	       =>  'DBD::Recall',
	       'AUTHOR'        =>  'Ashish Gulhati <hash@netropolis.org>',
	       'ABSTRACT_FROM' =>  'Recall.pm',
	       'VERSION_FROM'  =>  'Recall.pm',
	       'PREREQ_PM'     =>  {
				    'Carp'                         =>  0,
				    'Data::Dumper'                 =>  0,
				    'Replication::Recall::Client'  =>  0,
				   },
	      );
