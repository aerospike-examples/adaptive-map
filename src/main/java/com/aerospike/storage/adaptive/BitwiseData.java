package com.aerospike.storage.adaptive;

/**
 * Perform bit-wise operations (get, set) on an array of bytes. The array will grow as needed based on 
 * the operations performed, but never shrink smaller.
 * @author Tim
 *
 */
public class BitwiseData {
	private byte[] data;
	
	public BitwiseData() {
		super();
		data = new byte[1];
	}
	
	public BitwiseData(final byte[] initialValue) {
		this.data = initialValue;
	}
	
	private byte[] ensureCapacity(byte[] data, int capacity) {
		int oldSize = data == null ? 0 : data.length;
		if (capacity > oldSize) {
			byte[] newBuffer = new byte[capacity];
			
			for (int i = 0; data != null && i < capacity; i++) {
				newBuffer[i] = (i < oldSize) ? data[i] : 0; 
			}
			return newBuffer;
		}
		else {
			return data;
		}
	}
	
	/**
	 * Determine if a particular bit in the byte array is set. Returns true if set, false otherwise.
	 * <p>
	 * This operation will never change the underlying array.
	 * @param key - the byte array
	 * @param bit - the bit number to check, starting from 0
	 * @return true if the bit is set, false otherwise.
	 */
	protected boolean getBit(byte[] key, int bit) {
		int bite = bit >> 3;
		int bitInByte = bit - (bite<<3);

		if (key == null || bite >= key.length) {
			return false;
		}
		return ((key[bite] >> bitInByte) & 0x1) > 0;
	}
	
	/**
	 * Set an appropriate bit in the underlying byte array. This method can grow the underlying array to accomodate the bit being inspected
	 * @param key - the byte array
	 * @param bit - the bit number to set, starting from 0
	 * @return An array with the bit set. This may be the original array or a copy of it if the array had to grow.
	 */
	protected byte[] setBit(byte[] key, int bit) {
		int bite = bit >> 3;
		int bitInByte = bit - (bite<<3);
		byte[] data = ensureCapacity(key, bite+1);
		data[bite] |=  (1<< bitInByte);
		return data;
	}
	
	/**
	 * Clear a appropriate bit in the underlying byte array. This method can grow the underlying array to accomodate the bit being inspected
	 * @param key - the byte array
	 * @param bit - the bit number to clear, starting from 0
	 * @return An array with the bit set. This may be the original array or a copy of it if the array had to grow.
	 */
	protected byte[] clearBit(byte[] key, int bit) {
		int bite = bit >> 3;
		int bitInByte = bit - (bite<<3);
		byte[] data = ensureCapacity(key, bite+1);
		data[bite] &=  ~(1<< bitInByte);
		return data;
	}
	
	public boolean get(int bitNum) {
		return this.getBit(this.data, bitNum);
	}

	public BitwiseData set(int bitNum, boolean value) {
		this.data = value ? setBit(this.data, bitNum) : clearBit(this.data, bitNum);
		return this;
	}
}
