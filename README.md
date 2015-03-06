# Map Reduce for Integer Factorization

A Hadoop Map Reduce batch job for factoring large integers. Uses the quadratic sieve factorization algorithm.

The paper explaining the quadratic sieve algorithm and the Map Reduce approach to this implementation is here:

http://www.javiertordable.com/files/MapreduceForIntegerFactorization.pdf

## Abstract

Integer factorization is a very hard computational problem. Currently no efficient algorithm for integer factorization is publicly known. However, this is an important problem on which it relies the security of many real world cryptographic systems.

I present an implementation of a fast factorization algorithm on Map Reduce. Map Reduce is a programming model for high performance applications developed originally at Google. The quadratic sieve algorithm is split into the different Map Reduce phases and compared against a standard implementation.

## Description

I developed a basic implementation of the Quadratic Sieve Map Reduce which runs on Hadoop. Hadoop is an open source implementation of the Map Reduce framework. It is made in Java and it has been used effectively in configurations ranging from one to a few thousand computers. It is also available as a commercial cloud service.

This implementation is simply a proof of concept. It relies too heavily on the Map Reduce framework and it is severely bound by IO. However the size and complexity of the implementation are several orders of magnitude lower than many competing alternatives.

The 3 parts of the program are :

* Controller: Is the master job executed by the platform. It runs before spawning any worker job. It has two basic functions: first it generates the factor base. The factor base is serialized and passed to the workers in the context. Second it generates the full interval to sieve. All the data is stored in a single file in the distributed Hadoop file system. It then relies on the Map Reduce framework to automatically split it in an adequate number of shards and distribute it to the workers
* Mapper: The mappers perform the sieve. Each one of them receives an interval to sieve, and they return a subset of the elements in that input sieve which are smooth over the factor base. All output elements of all mappers share the same key
* Reducer: The reducer receives the set of smooth numbers and attempts to find a subset of them whose product is a square by solving the system modulo 2 using direct bit manipulation. If it finds a suitable subset, it tries to factor the original number, N. In general there will be many subsets to choose from. In case that the factorization is not successful with one of them, it proceeds to use another one. The single output is the factorization

In order to compare performance I developed another implementations of the Quadratic Sieve algorithm in Maple. Both implementations are basic in the sense that they implement the basic algorithm described above and the code has not been heavily optimized for performance. There are many differences between the two frameworks used that could impact performance. Because of that a direct comparison of running times or memory space may not be meaningful. However it is interesting to notice how each of the implementations scales depending on the size of the problem.

## References

This was part of the work towards my Ph.D. in Mathematics. I have a page with a few more projects:

http://www.javiertordable.com/

Hadoop is free and open source, you can get it here:

http://hadoop.apache.org/
