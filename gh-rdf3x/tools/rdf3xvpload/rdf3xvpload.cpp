#include <iostream>
#include <vector>
#include <map>
#include <fstream>
#include <cstdlib>
#include <sstream>
#include <queue>
#include <cstring>
#include "rdf3xload.cpp"
#include <unordered_set>
#include <unordered_map>
#include <list>
#include <mpi.h>
#if defined(OPEN_MPI) && OPEN_MPI
#include <mpi-ext.h>
#endif
#include <sys/time.h>
#include <algorithm>
#include <vector>
#include <exception>
#include <climits>

//---------------------------------------------------------------------------
//// RDF-3X
//// (c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x
////
//// This work is licensed under the Creative Commons
//// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
//// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
//// or send a letter to Creative Commons, 171 Second Street, Suite 300,
//// San Francisco, California, 94105, USA.
////---------------------------------------------------------------------------
//
using namespace std;

bool smallAddressSpace()
	// Is the address space too small?
{
	return sizeof(void*)<8;
}

class DataPartitioner {

	int numPartitions;
	int partId;
	bool isDirected;
	char *inputFileName;
	ifstream in;
	int maxDegree; // Sets threshold for ignoring high degree vertices
	uint64_t inputTripleCount;	
	uint64_t totalSubObjCount;	// Gives the number of vertices in the input graph
	int totalPredCount;		// Gives the unique number of edges
	uint64_t partitionSize;		// Gives the approximate size of a partition
	int startVertex;
	int endVertex;
	int *src,*dest,*edge;
	int *integerIdMap;
	int hopCount;
	
	int *outDegreeArray;
	uint64_t *vertexBlock;
	uint64_t *offsetArr;
	uint64_t adjListSize;
	pair <int, int> *adjList;

	unordered_map <string, int> subObjMap;
	unordered_map <int, string> revSubObjMap;
	unordered_map <string, int> predMap;
	unordered_map <int, string> revPredMap;

	list <int> srcList;
	list <int> predList;
	list <int> destList;

	void printArr(int *arr, int size);
	void addToAdjList(int subId, int objId, int predId);
	uint64_t addTriplesToPartition(int partId, bool isSilent);	
	string getPartitionFilePath(int id, bool isDb); 
	bool createReverseIdMap();
	void addTriplesUsingPartitionedGraph(int partId, bool isSilent);
	public:
		DataPartitioner(char *file, int partId, int numPartitions);
		bool loadPartitionedGraph();
		bool loadPartitionedGraphWithOrgFile(char *orgFileName);
		bool createInputStream();
		void createAdjList(); 
		void getReplicationRatio();
		bool parseAndCreateGraph();
		void printGraphToFile();
		void createPartitions(bool isSilen);
		bool loadIntegerTriples();
		void nHopHelper(int currVertex, int hopCount, bool *vertexBitMap);
};

//Constructor
DataPartitioner::DataPartitioner(char *file, int partId, int numPartitions) {
	this->partId = partId;
	this->numPartitions = numPartitions;
	this->inputFileName = file;

	maxDegree = INT_MAX; // Sets threshold for ignoring high degree vertices
	inputTripleCount = 0;	
	totalSubObjCount = 0;	// Gives the number of vertices in the input graph
	totalPredCount = 0;		// Gives the unique number of edges
	partitionSize = 0;		// Gives the approximate size of a partition

	hopCount = 1; // Setting the n-hop value
	isDirected = true; // true implements directed hop guarantee 
}

bool DataPartitioner::createInputStream() {

	in.open(inputFileName);
	cout << "Reading from " << inputFileName << endl;
	if(!in.is_open()) {
		cerr << "Unable to open " << inputFileName << endl;
		return false;
	}
	return true;
}

// Loads ID->Literal map from file
bool DataPartitioner::createReverseIdMap() {
	
	ifstream mapIn;
	mapIn.open("stringLiteral_integer_map");
	if(!mapIn.is_open()) {
		cerr << "Unable to open " << "stringLiteral_integer_map" << endl;
		return false;
	}
	
	int subObjMapSize, predMapSize, i=0, literalId;
	string literal;
	mapIn >> subObjMapSize;

	while(i<subObjMapSize) {
		mapIn >> literalId;
		getline(mapIn, literal);
		revSubObjMap[literalId] = literal;
		i++;
	}

	i=0;
	mapIn >> predMapSize;
	while(i<predMapSize) {
		mapIn >> literalId;
		getline(mapIn, literal);
		revPredMap[literalId] = literal;
		i++;
	}

	mapIn.close();
	return true;
}

bool DataPartitioner::loadIntegerTriples() {
	if (!createInputStream())
	       return false;	

	in >> totalSubObjCount;
	in >> inputTripleCount;
	
	src = new int[inputTripleCount];
	edge = new int[inputTripleCount];
	dest = new int[inputTripleCount];

	int i=0;
	while(!in.eof()) {
		in >> src[i]; in >> edge[i]; in >> dest[i];

		i++;
	}
	in.close();

	if(!createReverseIdMap())
		return false;
	return true;
}
	
// This function is used when the graph is already partitioned and there
// is no mapping between the original graph and the partitioned file 
bool DataPartitioner::loadPartitionedGraphWithOrgFile(char *orgFileName) {

	if (!createInputStream())
	       return false;	

	in >> totalSubObjCount;
	in >> inputTripleCount;
	in >> numPartitions;

	cout << "Total vertex count read = " << totalSubObjCount << endl;
	cout << "Total triples read = " << inputTripleCount << endl;
	cout << "Number of partition read = " << numPartitions << endl;

	vertexBlock = new uint64_t [numPartitions+1];
	for(int i=0; i<=numPartitions; i++) {
		in >> vertexBlock[i];
	}
	startVertex = vertexBlock[partId-1];
	endVertex = vertexBlock[partId];

// ----------------------
	ifstream orgFile;
	int s,e,d;
	integerIdMap = new int[totalSubObjCount];
	orgFile.open(orgFileName);
	if(!orgFile.is_open()) {       
		cerr << "Unable to open " << orgFileName << endl;
		return false;     
	}                         
	orgFile >> e; orgFile >> e;
// ----------------------

	src = new int[inputTripleCount];
	edge = new int[inputTripleCount];
	dest = new int[inputTripleCount];
	
	int i=0;
	while(i<inputTripleCount) {
		in >> src[i]; in >> edge[i]; in >> dest[i];

// ----------------------
		orgFile >> s; orgFile >> e; orgFile >> d;
		integerIdMap[src[i]] = s;
		integerIdMap[dest[i]] = d;
// ----------------------

		i++;
	}
	in.close();
	orgFile.close();
	
	if(!createReverseIdMap())
		return false;
	return true;
}

// Loads the partitioned graph
// We need to directly apply the n-hop guarantee
// after loading the graph
bool DataPartitioner::loadPartitionedGraph() {

	if (!createInputStream())
	       return false;	

	in >> totalSubObjCount;
	in >> inputTripleCount;
	in >> numPartitions;

	cout << "Total vertex count read = " << totalSubObjCount << endl;
	cout << "Total triples read = " << inputTripleCount << endl;
	cout << "Number of partition read = " << numPartitions << endl;

	vertexBlock = new uint64_t [numPartitions+1];
	for(int i=0; i<=numPartitions; i++) {
		in >> vertexBlock[i];
	}

	src = new int[inputTripleCount];
	edge = new int[inputTripleCount];
	dest = new int[inputTripleCount];
	
	int i=0;
	while(i<inputTripleCount) {
		in >> src[i]; in >> edge[i]; in >> dest[i];
		i++;
	}
	in.close();
	
	return true;
}

void DataPartitioner::printArr(int *arr, int size) {
	
	for(uint64_t i=0; i<size; i++) {
		cout << arr[i] << " " ;
	}
	cout << endl;
}

// Creates adjList from arrays src, dest and edge.
// Also uses totalSubObjCount, inputTripleCount
void DataPartitioner::createAdjList() {
	outDegreeArray = new int[totalSubObjCount]();
	offsetArr = new uint64_t[totalSubObjCount];

	// Creating the outdegree array
	for(uint64_t i=0; i<inputTripleCount; i++) {
		outDegreeArray[src[i]]++;
		if(!isDirected)
			outDegreeArray[dest[i]]++; // creating undirected adjList to aid n-hop
	}
	
	// Will be using the outDegreeArray to remove high degree nodes
	copy(outDegreeArray, outDegreeArray+totalSubObjCount, offsetArr);
//	printArr(outDegreeArray, totalSubObjCount);

	// PrefixSun to get the offset array
	for(uint64_t i=1; i<totalSubObjCount; i++) {
		offsetArr[i] += offsetArr[i-1];
	}
	
	uint64_t *tempOffsetArr = new uint64_t[totalSubObjCount];
	copy(offsetArr, offsetArr+totalSubObjCount, tempOffsetArr);
	
	adjListSize = offsetArr[totalSubObjCount-1];
	adjList = new pair <int, int> [adjListSize];

	// Creating adjList from all the input triples
	for(uint64_t i=0; i<inputTripleCount; i++) {
		adjList[--tempOffsetArr[src[i]]] = make_pair(dest[i], edge[i]);
		if(!isDirected)
			adjList[--tempOffsetArr[dest[i]]] = 
				make_pair(src[i], edge[i]); // creating undirected adjList to aid n-hop
	}

	// deleting unused memory
	delete []tempOffsetArr;
}

void DataPartitioner::nHopHelper(int currVertex, int hop, bool *vertexBitMap) {
	vertexBitMap[currVertex] = true;
	
	if(hop == 0)
		return;

	uint64_t offsetStart = 0;
	if(currVertex)
		offsetStart = offsetArr[currVertex-1];
	uint64_t offsetend = offsetArr[currVertex];

	for(uint64_t j=offsetStart; j<offsetend; j++) {
		nHopHelper(adjList[j].first, hop-1, vertexBitMap);
	}

}

void DataPartitioner::addTriplesUsingPartitionedGraph(int partId, bool isSilent) {
	ofstream out;
	out.open(getPartitionFilePath(partId, false).c_str());
	
	// Step1
	// Initialize bitmap and mark all vertices within n-1 hops	
	bool *vertexBitMap = new bool[totalSubObjCount]();
	uint64_t currVertex = vertexBlock[partId-1];
	uint64_t endVertex = vertexBlock[partId];

	while(currVertex < endVertex) {
		nHopHelper(currVertex, hopCount-1, vertexBitMap);
		currVertex++;
	}

	// Step2
	// Iterate through all the triples and print ones whose
	// src or dest is true in vertexBitMap
	for(uint64_t i=0; i<inputTripleCount; i++) {
		if(vertexBitMap[src[i]] || vertexBitMap[dest[i]]) { // For undirected hop guarantee 
	//	if(vertexBitMap[src[i]]) { // For directed hop guarantee 
			if(outDegreeArray[src[i]] < maxDegree && 
					outDegreeArray[dest[i]] < maxDegree) {
				if(!isSilent)
					out << revSubObjMap[integerIdMap[src[i]]] << " " << 
						revPredMap[edge[i]] << " " << 
						revSubObjMap[integerIdMap[dest[i]]] << " ." 
						<< endl;
			}
		}
	}

	out.close();
}
// To gather all the 2hop edges :
// Step1: Mark all the 1hop undirected verties
// Step2: Get all edges whose src or dest is marked
uint64_t DataPartitioner::addTriplesToPartition(int partId, bool isSilent) {

	ofstream out;
	out.open(getPartitionFilePath(partId, false).c_str());

	// Step1
	// Initialize bitmap and mark all vertices within n-1 hops	
	bool *vertexBitMap = new bool[totalSubObjCount]();
	uint64_t currVertex = partId-1;
	while(currVertex < totalSubObjCount) {
		nHopHelper(currVertex, hopCount-1, vertexBitMap);
		currVertex += numPartitions;
	}

	// Step2
	// Iterate through all the triples and print ones whose
	// src or dest is true in vertexBitMap
	uint64_t tripleCount = 0;
	if(isSilent) {
		if(isDirected) {
			for(uint64_t i=0; i<inputTripleCount; i++) {
				if(vertexBitMap[src[i]]) {
					tripleCount++;
				}
			}
		} else {
			for(uint64_t i=0; i<inputTripleCount; i++) {
				if(vertexBitMap[src[i]] || vertexBitMap[dest[i]]) {
					tripleCount++;
				}
			}
		}
	} else {
		if(isDirected) {
			for(uint64_t i=0; i<inputTripleCount; i++) {
				if(vertexBitMap[src[i]]) {
					tripleCount++;
					out << revSubObjMap[src[i]] << " " << 
						revPredMap[edge[i]] << " " << 
						revSubObjMap[dest[i]] << " ." 
						<< endl;
				}
			}
		} else {
			for(uint64_t i=0; i<inputTripleCount; i++) {
				if(vertexBitMap[src[i]] || vertexBitMap[dest[i]]) {
					tripleCount++;
					out << revSubObjMap[src[i]] << " " << 
						revPredMap[edge[i]] << " " << 
						revSubObjMap[dest[i]] << " ." 
						<< endl;
				}
			}
		}
	}
	out.close();
	return tripleCount;
}

void DataPartitioner::getReplicationRatio() {
	
	uint64_t totalTriples = 0;
	while(partId <= numPartitions) {

		cout << "Counting triples for partition -- " << partId << endl;
		uint64_t tmpCount = addTriplesToPartition(partId, true); // reusing this function just to get the triple count
		
		cout << "Triple Count for partition " << partId << " is " << tmpCount << endl;
		cout << endl;

		totalTriples += tmpCount;
		partId++;
	}

	cout << "Total triple Count across all partitions = " << totalTriples << endl;
	cout << "Input triple count = " << inputTripleCount << endl;
	cout << "Replication Ratio = " << totalTriples*1.0/inputTripleCount << endl;
}

string DataPartitioner::getPartitionFilePath(int id, bool isDb) {
	ostringstream outPath;
	outPath << numPartitions << "/" << inputFileName << "_" << id;
	if(isDb)
		outPath << "db";
	return outPath.str();
}

void DataPartitioner::createPartitions(bool isSilent) {
	
	uint64_t totalTriples = 0;
	// Uses hash partitioning
	totalTriples += addTriplesToPartition(partId, isSilent);	
	// Uses partitioned graph to add triples
	//addTriplesUsingPartitionedGraph(partId, isSilent);	

	if(!isSilent) {
		// Loading partition into database		
		string filePath = getPartitionFilePath(partId, false);
		string dbPath = getPartitionFilePath(partId, true);

		cout << "Loading partition " << filePath << " into database " << dbPath << endl;
		rdf3xload_serial(dbPath.c_str(), filePath.c_str());
	}
}

int main(int argc,char* argv[])
{
	if (smallAddressSpace())
		cerr << "Warning: Running RDF-3X on a 32 bit system is not supported and will fail for large data sets. Please use a 64 bit system instead!" << endl;

	if (argc<2) {
		cerr <<  "usage: " << argv[0] << " [inputFile] " << endl
			<< "without input file data is read from stdin" << endl
			<< "number of partitions is the number of cores requested" << endl;
		return 1;
	}

	cerr << "RDF-3X turtle importer" << endl
		<< "(c) 2008 Thomas Neumann. Web site: http://www.mpi-inf.mpg.de/~neumann/rdf3x" << endl;

	MPI::Init (argc, argv);
	int partId = MPI::COMM_WORLD.Get_rank( ) + 1;	
	int numPartitions = MPI::COMM_WORLD.Get_size ( ); 

	DataPartitioner dataPartitioner(argv[1], partId, numPartitions);
	
//	bool noError = dataPartitioner.loadPartitionedGraph();
//	bool noError = dataPartitioner.loadPartitionedGraphWithOrgFile(argv[3]);
	bool noError = dataPartitioner.loadIntegerTriples();
	if(!noError) {
		cerr << "Error in parsing input file" << endl;
		MPI_Finalize();
		return 1;
	}
	dataPartitioner.createAdjList();
	
	dataPartitioner.createPartitions(false);
//	dataPartitioner.getReplicationRatio();
	
	MPI_Finalize();
	return 0;
}

