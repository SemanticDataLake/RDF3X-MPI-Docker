# RDF3X-MPI-Docker

RDF3x-MPI is an MPI version of the GH-RDF3x (https://bitbucket.org/saikrishnan/rdf3x-mpi)

# USAGE

``docker run -it --rm -v /path/to/output/folder:/data kemele/rdf3x-mpi:1.0 rdf3xvpload rdffile.nt numparts``

``docker run -it --rm -v /path/to/output/folder:/data kemele/rdf3x-mpi:1.0 rdf3xvpquery rdffile.nt numparts``
