/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2002-2012, Open Source Geospatial Foundation (OSGeo)
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
package org.geotools.data.ngi;

import java.awt.RenderingHints.Key;
import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.AbstractDataStoreFactory;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.data.DataUtilities;
import org.geotools.referencing.CRS;
import org.geotools.util.KVP;
import org.geotools.util.logging.Logging;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * NGI DataStoreFactory
 * 
 * @author MapPlus, mapplus@gmail.com, http://onspatial.com
 * @since 2012-10-30
 * @see
 * 
 */
public class NGIDataStoreFactory extends AbstractDataStoreFactory implements DataStoreFactorySpi {
    protected static final Logger LOGGER = Logging.getLogger(NGIDataStoreFactory.class);

    // 공간데이터(*.NGI, *.NBI), 속성데이터(*.NDA, *.NDB) 표현
    static final String FILE_TYPE = "ngi";

    public static final Param PARAM_FILE = new Param("url", URL.class, "url to a .ngi/.nda file",
            true, null, new KVP(Param.EXT, FILE_TYPE));

    public static final Param PARAM_SRS = new Param("srs", String.class, "force srs", false, "",
            new KVP(Param.LEVEL, "advanced"));

    public static final Param PARAM_CHARSET = new Param("charset", String.class,
            "character used to decode strings from the NGI file", false, "x-windows-949", new KVP(
                    Param.LEVEL, "advanced"));

    public String getDisplayName() {
        return "NGI File";
    }

    public String getDescription() {
        return "NGI ASCII Format (*.ngi)";
    }

    public Param[] getParametersInfo() {
        return new Param[] { PARAM_FILE, PARAM_SRS, PARAM_CHARSET };
    }

    public boolean isAvailable() {
        return true;
    }

    @SuppressWarnings("unchecked")
    public Map<Key, ?> getImplementationHints() {
        return Collections.EMPTY_MAP;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public boolean canProcess(Map params) {
        boolean result = false;
        if (params.containsKey(PARAM_FILE.key)) {
            try {
                URL url = (URL) PARAM_FILE.lookUp(params);
                result = url.getFile().toLowerCase().endsWith(".ngi");
            } catch (IOException ioe) {
                /* return false on any exception */
            }
        }
        return result;
    }

    public DataStore createDataStore(Map<String, Serializable> params) throws IOException {
        URL url = (URL) PARAM_FILE.lookUp(params);
        String code = (String) PARAM_SRS.lookUp(params);
        String charset = (String) PARAM_CHARSET.lookUp(params);

        if (charset == null || charset.isEmpty()) {
            charset = (String) PARAM_CHARSET.sample;
        }

        CoordinateReferenceSystem crs = null;
        if (code == null || code.isEmpty()) {
            crs = null; // default??
        } else {
            try {
                crs = CRS.decode(code);
            } catch (NoSuchAuthorityCodeException e) {
                LOGGER.log(Level.WARNING, e.getMessage(), e);
            } catch (FactoryException e) {
                LOGGER.log(Level.WARNING, e.getMessage(), e);
            }
        }

        return new NGIDataStore(DataUtilities.urlToFile(url), Charset.forName(charset), crs);
    }

    public DataStore createNewDataStore(Map<String, Serializable> params) throws IOException {
        return null;
    }

}