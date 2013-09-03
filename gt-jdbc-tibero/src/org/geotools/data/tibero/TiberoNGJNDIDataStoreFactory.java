/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2002-2008, Open Source Geospatial Foundation (OSGeo)
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package org.geotools.data.tibero;

import java.util.Map;

import org.geotools.jdbc.JDBCJNDIDataStoreFactory;

@SuppressWarnings("unchecked")
public class TiberoNGJNDIDataStoreFactory extends JDBCJNDIDataStoreFactory {

    public TiberoNGJNDIDataStoreFactory() {
        super(new TiberoNGDataStoreFactory());
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected void setupParameters(Map parameters) {
        super.setupParameters(parameters);

        parameters.put(TiberoNGDataStoreFactory.LOOSEBBOX.key, TiberoNGDataStoreFactory.LOOSEBBOX);
        parameters.put(TiberoNGDataStoreFactory.PREPARED_STATEMENTS.key,
                TiberoNGDataStoreFactory.PREPARED_STATEMENTS);
        parameters.put(MAX_OPEN_PREPARED_STATEMENTS.key, MAX_OPEN_PREPARED_STATEMENTS);
    }
}
