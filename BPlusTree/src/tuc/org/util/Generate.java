package tuc.org.util;

public class Generate {
        /**
         * Generates an array of random integers
         * @param NO_OF_ELEMENTS Number of elements in the array
         * @return the array of integers
         */
        public int[] RandomInts(int START_INT,int END_INT,int NO_OF_ELEMENTS) {
            java.util.Random randomGenerator = new java.util.Random();
            int[] randomInts = randomGenerator.ints(START_INT, END_INT).distinct().limit(NO_OF_ELEMENTS).toArray();
            return  randomInts;
        }

    }

