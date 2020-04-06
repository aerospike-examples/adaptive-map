/*
 * Copyright 2012-2020 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.storage.adaptive;

/**
 * When a block (record) grows too large, it splits into 2 records containing around half the records
 * in each block, then the original record is removed. The top level record is considered block 0 for
 * this purpose.
 * <p/> 
 * Block 0 splits to give blocks 1,2. <br>
 * Block 1 splits to give blocks 3,4 <br>
 * Block 2 splits to give blocks 5,6 <br>
 * etc
 * <p/>
 * The general rule is if block N splits, its children are 2N+1, 2N+2
 * <p>
 * In order to know which blocks have split, block 0 also keeps a header bitmap. This bitmap stores
 * a 1 if the block has split, 0 otherwise. 
 * <p>
 * <b>Note:</b> At the moment, there is no way of "unsplitting" a block if it shrinks down either by 
 * explicit removal of elements or by having items TTL out
 * <p>
 * When a block splits, the records must be divvied up into the 2 new sub-blocks in a deterministic fashion.
 * This is done by using a RIPEMD160 hash of the map key (or other algorithm) to give a byte[] representing
 * that element. The K<sup>th</sup> bit of this hash is checked: if it is clear, the record belongs in sub-block
 * 2N+1, otherwise 2N+2. K here is the level of the tree (int)(log2N(N+1)-1).
 */

public interface Hash {
	public byte[] getHash(Object key);
}