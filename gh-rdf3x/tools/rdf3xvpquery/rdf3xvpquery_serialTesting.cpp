#include "cts/codegen/CodeGen.hpp"
#include "cts/infra/QueryGraph.hpp"
#include "cts/parser/SPARQLLexer.hpp"
#include "cts/parser/SPARQLParser.hpp"
#include "cts/plangen/PlanGen.hpp"
#include "cts/semana/SemanticAnalysis.hpp"
#include "infra/osdep/Timestamp.hpp"
#include "rts/database/Database.hpp"
#include "rts/runtime/Runtime.hpp"
#include "rts/operator/Operator.hpp"
#include "rts/operator/PlanPrinter.hpp"
#ifdef CONFIG_LINEEDITOR
#include "lineeditor/LineInput.hpp"
#endif
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <sys/time.h>
#include <stdio.h>
#include <mpi.h>
#include <sstream>
#include <cstring>
//---------------------------------------------------------------------------
// RDF-3X
// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------
using namespace std;
//---------------------------------------------------------------------------
bool smallAddressSpace()
	// Is the address space too small?
{
	return sizeof(void*)<8;
}

static bool readLine(string& query)
	// Read a single line
{
#ifdef CONFIG_LINEEDITOR
	// Use the lineeditor interface
	static lineeditor::LineInput editHistory(L">");
	return editHistory.readUtf8(query);
#else
	// Default fallback
	cerr << ">"; cerr.flush();
	return getline(cin,query);
#endif
}
//---------------------------------------------------------------------------
static void showHelp()
	// Show internal commands
{
	cout << "Recognized commands:" << endl
		<< "help          shows this help" << endl
		<< "select ...    runs a SPARQL query" << endl
		<< "explain ...   shows the execution plan for a SPARQL query" << endl
		<< "exit          exits the query interface" << endl;
}
//---------------------------------------------------------------------------

static void runQuery(Database& db,const string& query,bool explain)
	// Evaluate a query
{
	struct timeval tim;
	gettimeofday (&tim, NULL);
	double t1=tim.tv_sec+(tim.tv_usec/1000000.0);
	QueryGraph queryGraph;
	{
		// Parse the query
		SPARQLLexer lexer(query);
		SPARQLParser parser(lexer);
		try {
			parser.parse();
		} catch (const SPARQLParser::ParserException& e) {
			cerr << "parse error: " << e.message << endl;
			return;
		}

		// And perform the semantic anaylsis
		try {
			SemanticAnalysis semana(db);
			semana.transform(parser,queryGraph);
		} catch (const SemanticAnalysis::SemanticException& e) {
			cerr << "semantic error: " << e.message << endl;
			return;
		}
		if (queryGraph.knownEmpty()) {
			if (explain)
				cerr << "static analysis determined that the query result will be empty" << endl; else
					cout << "<empty result>" << endl;
			return;
		}
	}
	gettimeofday(&tim, NULL);
	double t2=tim.tv_sec+(tim.tv_usec/1000000.0);
//	printf("%.10lf ms - for query parsing\n", (t2-t1)*1000);

	gettimeofday (&tim, NULL);
	t1=tim.tv_sec+(tim.tv_usec/1000000.0);
	// Run the optimizer
	PlanGen plangen;
	Plan* plan=plangen.translate(db,queryGraph);
	if (!plan) {
		cerr << "internal error plan generation failed" << endl;
		return;
	}
	gettimeofday(&tim, NULL);
	t2=tim.tv_sec+(tim.tv_usec/1000000.0);
//	printf("%.10lf ms -- running optimizer\n", (t2-t1)*1000);

	// Build a physical plan
	Runtime runtime(db);
	gettimeofday (&tim, NULL);
	t1=tim.tv_sec+(tim.tv_usec/1000000.0);
	Operator* operatorTree=CodeGen().translate(runtime,queryGraph,plan,false);//isSilent
	gettimeofday(&tim, NULL);
	t2=tim.tv_sec+(tim.tv_usec/1000000.0);
//	printf("%.10lf ms -- Codegen translater\n", (t2-t1)*1000);


	// Explain if requested
	if (explain) {
		DebugPlanPrinter out(runtime,false);
		operatorTree->print(out);
	} else {
		// Else execute it
	gettimeofday (&tim, NULL);
	t1=tim.tv_sec+(tim.tv_usec/1000000.0);
		if (operatorTree->first()) {
			while (operatorTree->next()) ;
		}
	gettimeofday(&tim, NULL);
	t2=tim.tv_sec+(tim.tv_usec/1000000.0);
//	printf("%.10lf ms -- Silent Printing\n", (t2-t1)*1000);
	}

	delete operatorTree;
}

//---------------------------------------------------------------------------
int main(int argc,char* argv[])
{
	string query;
	char queryArr[1000];
	struct timeval tim;
	double t1, t2;

	//  Initialize MPI.
//	MPI::Init ( argc, argv );

	//  Get the number of processes.
//	int numPartitions = MPI::COMM_WORLD.Get_size ( );
	
	//  Determine the rank of this process.
//	int processorId = MPI::COMM_WORLD.Get_rank ( );

	for(int processorId = 0; processorId <16; processorId++) {
	if(processorId == 0) {
		if (smallAddressSpace())
			cerr << "Warning: Running RDF-3X on a 32 bit system is not supported and will fail for large data sets. Please use a 64 bit system instead!" << endl;

		// Greeting
		cerr << "RDF-3X query interface" << endl
			<< "(c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x" << endl;
	}

	ostringstream dbName;
	dbName << argv[1] << processorId + 1 << "db"; 
	// Open the database
	Database db;
	if (!db.open(dbName.str().c_str(),true)) {
		cerr << "unable to open database " << dbName.str() << endl;
		return 1;
	}
	

	while (true) {
		if(processorId == 0) {
			// No, accept user input
			cerr << "Enter 'help' for instructions" << endl;
			if (!readLine(query))
				break;
			if (query=="") continue;

			if ((query=="quit")||(query=="exit")) {
				exit(0);
			} else if (query=="help") {
				showHelp();
			} 
			strcpy(queryArr, query.c_str());
		}

//		MPI_Bcast((void *)queryArr, 1000, MPI_CHAR, 0, MPI_COMM_WORLD);

		string tmp(queryArr);

		if(processorId == 0) {
			gettimeofday (&tim, NULL);
			t1=tim.tv_sec+(tim.tv_usec/1000000.0);  
		}
//		cerr << "processorid  " << processorId << " " << tmp << endl; 	
		//string tmp(queryArr);
		runQuery(db, tmp, false);
//		MPI_Barrier(MPI_COMM_WORLD);
		
		if(processorId == 0) {
			gettimeofday (&tim, NULL);
			t2=tim.tv_sec+(tim.tv_usec/1000000.0);  
			printf("%.10lf milli seconds elapsed\n", (t2-t1)*1000);  
			cout << endl;
		}
		break;
	}
	cout.flush();
//	MPI_Finalize();
	}
	return 0;
}

