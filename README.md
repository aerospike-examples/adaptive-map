# Adaptive Map
## Concept
The Aerospike API contains a very flexible set of Complex Data Types (CDTs), embodied in Maps, Lists and Bitwise operations. However, for performance reasons Aerospike imposes finite size records limits; <= 1MB in earlier than 4.2, up to 8MB in 4.2+. There are many use cases where the vast majority of the data will fit easily inside a single record, but there are outlier cases where operations do not fit within these limits.

Take for example, time series data where there are typically in the dozens of data points per day, but can go up to 100,000. If each data point is 500 bytes, the vast majority of cases will be able to fit inside an Aerospike namespace with a [write-block-size](https://www.aerospike.com/docs/reference/configuration/#write-block-size) of 1MB. However, the largest records will require 50,000,000 of storage, far larger than can be stored in a single Aerospike record.

In many such cases, the larger records account for a small percentage of the data, but since they are storing more data they are typically transacted more frequently, giving an order n<sup>2</sup> performance degradation.

This project is designed to overcome these limitations for Maps. Effectively it allows storage of an unlimited set of data in a map by breaking a single monolithic record up into smaller records. Some of the features it offers are:
* Trivial performance degradation over native Aerospike if all data points fit within a single Aerospike record.
* O(1) insert and retrieval time for any single data point.
* Parallelization of retrieving multiple data points using Aerospike's batch gets and map operations

### Splitting Records
Let's assume for the sake of simplicity that our map can only hold 4 records, and we have 10 data points to store in a record with key *TestKey*:

1. Tim -> { Tim Record }
2. Bob -> { Bob Record }
3. Sue -> { Sue Record }
4. Tom -> { Tom Record }
5. Art -> { Art Record }
6. Aya -> { Aya Record }
7. Joe -> { Joe Record }
8. Don -> { Don Record }
9. Jim -> { Jim Record }
10. Sam -> { Sam Record }

In order to do the splitting, it takes the key of each record and runs it through [RIPEMD-160](https://homes.esat.kuleuven.be/~bosselae/ripemd160/pdf/AB-9601/AB-9601.pdf), a hash function which generates 20 bytes of noise called a digest, then divides the records up based on bits in the digest.

Let's see this with an example. Here is the set of records again, but this time with a digest added. (Note that for simplicity, only 1 byte of the 20 bytes is shown in binary)

1. Tim -> { Tim Record } Digest: 1111 0001
2. Bob -> { Bob Record } Digest: 1011 0010
3. Sue -> { Sue Record } Digest: 1001 0011
4. Tom -> { Tom Record } Digest: 0100 0011
5. Art -> { Art Record } Digest: 1110 1000
6. Aya -> { Aya Record } Digest: 1010 1001
7. Joe -> { Joe Record } Digest: 1110 0011
8. Don -> { Don Record } Digest: 0110 0100
9. Jim -> { Jim Record } Digest: 0110 0101
10. Sam -> { Sam Record } Digest: 0010 0000


The base record can hold 4 entries, so after the first 4 entries are added the record will look something like:

![TestKey](/images/SingleKey.png)

The problem occurs when inserting the next record. This map would then hold 5 entries, but it has been set up to hold only 4. It must split the record into 2 records.

In the map at the moment, there are 5 records: Tim, Bob, Sue, Tom and Art. In order to break these into 2 sub-maps, take the least significant bit of the digest and if it's a 0, put it in one sub-map and if it's a 1 put it in the other. Thus we end up with:

2. Bob -> { Bob Record } Digest: 1011 0010
5. Art -> { Art Record } Digest: 1110 1000

and

1. Tim -> { Tim Record } Digest: 1111 0001
3. Sue -> { Sue Record } Digest: 1001 0011
4. Tom -> { Tom Record } Digest: 0100 0011

The original record has a key of *TestKey*, so the sub-records will have keys of *TestKey:1* and *TestKey:2*. We no longer need this information in the root block (*TestKey*), so we can remove the items from there. However, we do need to record that the record has overflowed its data limit and hence has split. In order to do this efficiently, a bitmap is used. In this bitmap, a 1 will indicate the block has split, and 0 means the block either isn't full or doesn't exist:
 
![OneSplit](/images/OneSplit.png)

The next record (*Aya*) arrives, finds that the root block has split by the fact it has a 1 in the 0-bit in the bitmap. So it looks at the digest for the record (1010 1001), sees that it has a 1 in the 0-bit and hence puts in in *TestKey:2*. This makes it the fourth record in the map, which is fine. The maps now look like:

2. Bob -> { Bob Record } Digest: 1011 0010
5. Art -> { Art Record } Digest: 1110 1000

and

1. Tim -> { Tim Record } Digest: 1111 0001
3. Sue -> { Sue Record } Digest: 1001 0011
4. Tom -> { Tom Record } Digest: 0100 0011
6. Aya -> { Aya Record } Digest: 1010 1001

Record 7 (*Joe*) arrives. The same process is repeated and again it ends up in *TestKey:2* record as the LSB of the digest is 1. This is the 5th item in this record, so again the record needs to split. Bit 2 of the bitmap is set to 1, and *TestKey:5* and *TestKey:6* are created. The data is placed into these 2 records by looking at the *second* least significant bit, then *TestKey:2* is removed. 

![Two Splits](/images/TwoSplits.png)

Record 8 (*Don*) has a 0 in bit 0, so it is placed in *TestKey:1*, making it the 3rd entry there.

Record 9 (*Jim*, digest 0110 0101) has a 1 in bit 0, so it would go to *TestKey:2*, but bit 2 is also 1, showing that it has split. Hence bit 1 is consulted, it is 0, so it would go in *TestKey:5*. It's the 3rd entry in this map so no splits occur.

Record 10 (*Sam*, digest 0010 0000) has a 0 in bit 0, so it is placed in *TestKey:1*, making it the 4th entry there.

Thus, the final picture of the storage is:

![Final](/images/Final.png)

The nice thing about this approach is that, given the digest and the bitmap of split maps, the exact map to store or read a record from can be computed with no database accesses. This is true irrespective of how many times a map has split. When map N splits, it's children will always be map 2N+1 and 2N+2 so the key of children of a given map are easy to calculate.

Once a map splits, the original map which held that data is removed. This is true for all maps except the root map, which must remain there to maintain the bitmap.

_**Note:**_ The above description details some of the internal workings of the library. It should not matter to its use. Also, there are a lot of details glossed over, like locking which prevents 2 threads inserting 2 different records in the same block simultaneous and both causing it to split.

