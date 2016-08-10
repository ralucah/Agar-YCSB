/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.generator;

/**
 * A generator that is capable of generating ints as well as strings
 *
 * @author cooperb
 *
 */
public abstract class IntegerGenerator extends Generator {
    int lastint;

    /**
     * Set the last value generated. IntegerGenerator subclasses must use this call
     * to properly set the last string value, or the lastString() and lastInt() calls won't work.
     */
    protected void setLastInt(int last) {
        lastint = last;
    }

    /**
     * Return the next value as an int. When overriding this method, be sure to call setLastString() properly, or the lastString() call won't work.
     */
    public abstract int nextInt();

    /**
     * Generate the next string in the distribution.
     */
    public String nextString() {
        return "" + nextInt();
    }

    /**
     * Return the previous string generated by the distribution; e.g., returned from the last nextString() call.
     * Calling lastString() should not advance the distribution or have any side effects. If nextString() has not yet
     * been called, lastString() should return something reasonable.
     */
    @Override
    public String lastString() {
        return "" + lastInt();
    }

    /**
     * Return the previous int generated by the distribution. This call is unique to IntegerGenerator subclasses, and assumes
     * IntegerGenerator subclasses always return ints for nextInt() (e.g. not arbitrary strings).
     */
    public int lastInt() {
        return lastint;
    }

    /**
     * Return the expected value (mean) of the values this generator will return.
     */
    public abstract double mean();
}
