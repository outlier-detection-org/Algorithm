/*
 *      _______                       _        ____ _     _
 *     |__   __|                     | |     / ____| |   | |
 *        | | __ _ _ __ ___  ___  ___| |    | (___ | |___| |
 *        | |/ _` | '__/ __|/ _ \/ __| |     \___ \|  ___  |
 *        | | (_| | |  \__ \ (_) \__ \ |____ ____) | |   | |
 *        |_|\__,_|_|  |___/\___/|___/_____/|_____/|_|   |_|
 *                                                         
 * -----------------------------------------------------------
 *
 *  TarsosLSH is developed by Joren Six at 
 *  The School of Arts,
 *  University College Ghent,
 *  Hoogpoort 64, 9000 Ghent - Belgium
 *  
 * -----------------------------------------------------------
 *
 *  Info    : http://tarsos.0110.be/tag/TarsosLSH
 *  Github  : https://github.com/JorenSix/TarsosLSH
 *  Releases: http://tarsos.0110.be/releases/TarsosLSH/
 * 
 */
package be.tarsos.lsh;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import be.tarsos.lsh.families.HashFamily;
import be.tarsos.lsh.families.HashFunction;

/**
 * An index contains one or more locality sensitive hash tables. These hash
 * tables contain the mapping between a combination of a number of hashes
 * (encoded using an integer) and a list of possible nearest neighbours.
 *
 * @author Joren Six
 */
class HashTable implements Serializable {

    private static final long serialVersionUID = -5410017645908038641L;

    /**
     * Contains the mapping between a combination of a number of hashes (encoded
     * using an integer) and a list of possible nearest neighbours
     */
    private HashMap<Integer, List<Vector>> hashTable; //like the buckets
    private HashFunction[] hashFunctions;
    private HashFamily family;

    /**
     * Initialize a new hash table, it needs a hash family and a number of hash
     * functions that should be used.
     *
     * @param numberOfHashes The number of hash functions that should be used.
     * @param family The hash function family knows how to create new hash
     * functions, and is used therefore.
     */
    public HashTable(int numberOfHashes, HashFamily family) {
        hashTable = new HashMap<Integer, List<Vector>>();
        this.hashFunctions = new HashFunction[numberOfHashes];
        for (int i = 0; i < numberOfHashes; i++) {
            hashFunctions[i] = family.createHashFunction();
        }
        this.family = family;
    }

    /**
     * Query the hash table for a vector. It calculates the hash for the vector,
     * and does a lookup in the hash table. If no candidates are found, an empty
     * list is returned, otherwise, the list of candidates is returned.
     *
     * @param query The query vector.
     * @return Does a lookup in the table for a query using its hash. If no
     * candidates are found, an empty list is returned, otherwise, the list of
     * candidates is returned.
     */
    public List<Vector> query(Vector query) {
        Integer combinedHash = hash(query);
        if (hashTable.containsKey(combinedHash)) {
            List<Vector> r = hashTable.get(combinedHash);
            return r;
        } else {
            return new ArrayList<Vector>();
        }
    }

    /**
     * Add a vector to the index.
     *
     * @param vector
     */
    public void add(Vector vector) {
        Integer combinedHash = hash(vector);
        if (!hashTable.containsKey(combinedHash)) {
            hashTable.put(combinedHash, new ArrayList<Vector>());
        }
        hashTable.get(combinedHash).add(vector);
    }

    /**
     * Calculate the combined hash for a vector.
     *
     * @param vector The vector to calculate the combined hash for.
     * @return An integer representing a combined hash.
     */
    private Integer hash(Vector vector) {
        int hashes[] = new int[hashFunctions.length];
        for (int i = 0; i < hashFunctions.length; i++) {
            hashes[i] = hashFunctions[i].hash(vector);
        }
        Integer combinedHash = family.combine(hashes);
        return combinedHash;
    }

    /**
     * Return the number of hash functions used in the hash table.
     *
     * @return The number of hash functions used in the hash table.
     */
    public int getNumberOfHashes() {
        return hashFunctions.length;
    }

    void remove(Vector vector) {
        Integer combinedHash = hash(vector);
        if (hashTable.containsKey(combinedHash)) {
           
            
            hashTable.get(combinedHash).remove(vector);
        }
    }
}
