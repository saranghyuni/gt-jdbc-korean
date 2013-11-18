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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;
import org.geotools.factory.GeoTools;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.util.Converters;
import org.geotools.util.logging.Logging;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;

/**
 * NGI Reader
 * 
 * @author MapPlus, mapplus@gmail.com, http://onspatial.com
 * @since 2012-10-30
 * @see
 * 
 */
public class NGIReader extends AbstractNGIReader {
    protected static final Logger LOGGER = Logging.getLogger(NGIReader.class);

    CoordinateReferenceSystem crs;

    SimpleFeatureType schema;

    Class<?> geomBinding;

    boolean hasNext = false;

    BufferedReader ngiReader = null;

    BufferedReader ndaReader = null;

    GeometryFactory gf = JTSFactoryFinder.getGeometryFactory(GeoTools.getDefaultHints());

    SimpleFeatureBuilder fb;

    int featureID = 0;

    public NGIReader(File ngiFile, File ndaFile, Charset charset) throws IOException {
        this.ngiReader = new BufferedReader(new InputStreamReader(new FileInputStream(ngiFile),
                charset));
        if (ndaFile.exists()) {
            this.ndaReader = new BufferedReader(new InputStreamReader(new FileInputStream(ndaFile),
                    charset));
        }
    }

    public void close() {
        IOUtils.closeQuietly(ngiReader);
        IOUtils.closeQuietly(ndaReader);
    }

    public SimpleFeatureType getSchema() {
        return schema;
    }

    public void setSchema(SimpleFeatureType schema) {
        this.schema = schema;
        this.crs = schema.getCoordinateReferenceSystem();
        this.geomBinding = schema.getGeometryDescriptor().getType().getBinding();
        this.fb = new SimpleFeatureBuilder(schema);
        this.featureID = 0;

        seekLayer(ngiReader, schema.getTypeName());
        hasNext = nextRecord(ngiReader);
        if (ndaReader != null) {
            seekLayer(ndaReader, schema.getTypeName());
            nextRecord(ndaReader);
        }
    }

    public boolean hasNext() {
        return hasNext;
    }

    public SimpleFeature next() {
        SimpleFeature feature = fb.buildFeature(schema.getTypeName() + "." + ++featureID);
        Geometry geometry = getNextGeometry(ngiReader);

        if (geometry != null) {
            geometry.setUserData(crs);
            feature.setDefaultGeometry(geometry);
        }

        hasNext = nextRecord(ngiReader);
        if (ndaReader != null) {
            try {
                // 7371, "영광읍", "행정지명", "법정명", "1000035610069H00410000000000073716"
                String[] values = ndaReader.readLine().split(",");
                int addIndex = 0;
                for (int index = 0; index < schema.getAttributeCount(); index++) {
                    AttributeDescriptor desc = schema.getDescriptor(index);
                    if (desc instanceof GeometryDescriptor) {
                        continue;
                    }

                    Class<?> binding = desc.getType().getBinding();
                    if (binding.isAssignableFrom(String.class)) {
                        String value = (String) Converters.convert(values[addIndex++], binding);
                        if (value != null && !value.isEmpty()) {
                            value = value.trim();
                            value = value.substring(1, value.length() - 1);
                        }
                        feature.setAttribute(desc.getLocalName(), value);
                    } else {
                        Object value = Converters.convert(values[addIndex++], binding);
                        feature.setAttribute(desc.getLocalName(), value);
                    }
                }
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, e.getMessage(), e);
            }

            nextRecord(ndaReader);
        }
        return feature;
    }

    private Geometry getNextGeometry(BufferedReader reader) {
        try {
            String gtype = reader.readLine().toUpperCase().trim(); // LINESTRING
            if (gtype.startsWith("POINT")) {
                return gf.createPoint(parseCoordinate(reader.readLine()));
            } else if (gtype.startsWith("TEXT")) {
                return gf.createPoint(parseCoordinate(reader.readLine()));
            } else if (gtype.startsWith("LINE")) {
                int numofPoints = parseInteger(reader.readLine());
                Coordinate[] coordinates = new Coordinate[numofPoints];
                for (int index = 0; index < numofPoints; index++) {
                    coordinates[index] = parseCoordinate(reader.readLine());
                }
                return gf.createLineString(coordinates);
            } else if (gtype.startsWith("POLYGON")) {
                int numofRing = parseInteger(reader.readLine().replace("NUMPARTS", ""));
                LinearRing shell = null;
                LinearRing[] holes = numofRing > 1 ? new LinearRing[numofRing - 1] : null;
                for (int ringIndex = 0; ringIndex < numofRing; ringIndex++) {
                    int numofPoints = parseInteger(reader.readLine());
                    // =========================================================
                    // NGI 포맷에서는 폴리곤인 경우에도 시작점과 끝점이 다르다.
                    Coordinate[] coordinates = new Coordinate[numofPoints + 1];
                    // =========================================================
                    for (int index = 0; index < numofPoints; index++) {
                        coordinates[index] = parseCoordinate(reader.readLine());
                    }
                    coordinates[numofPoints] = coordinates[0];

                    if (ringIndex == 0) {
                        shell = gf.createLinearRing(coordinates);
                    } else {
                        holes[ringIndex - 1] = gf.createLinearRing(coordinates);
                    }
                }

                return gf.createPolygon(shell, holes);
            } else if (gtype.startsWith("MULTIPOINT")) {
                int numofPoints = parseInteger(reader.readLine());
                Coordinate[] coordinates = new Coordinate[numofPoints];
                for (int index = 0; index < numofPoints; index++) {
                    coordinates[index] = parseCoordinate(reader.readLine());
                }
                return gf.createMultiPoint(coordinates);
            } else if (gtype.startsWith("MULTILINE")) {
                int numofParts = parseInteger(reader.readLine().replace("NUMPARTS", ""));
                LineString[] lineStrings = new LineString[numofParts];
                for (int partIndex = 0; partIndex < numofParts; partIndex++) {
                    int numofPoints = parseInteger(reader.readLine());
                    Coordinate[] coordinates = new Coordinate[numofPoints];
                    for (int index = 0; index < numofPoints; index++) {
                        coordinates[index] = parseCoordinate(reader.readLine());
                    }
                    lineStrings[partIndex] = gf.createLineString(coordinates);
                }
                return gf.createMultiLineString(lineStrings);
            } else if (gtype.startsWith("MULTIPOLY")) {
                int numofParts = parseInteger(reader.readLine().replace("NUMPARTS", ""));
                Polygon[] polygons = new Polygon[numofParts];
                for (int partIndex = 0; partIndex < numofParts; partIndex++) {
                    int numofRing = parseInteger(reader.readLine().replace("NUMPARTS", ""));
                    LinearRing shell = null;
                    LinearRing[] holes = numofRing > 1 ? new LinearRing[numofRing - 1] : null;
                    for (int ringIndex = 0; ringIndex < numofRing; ringIndex++) {
                        int numofPoints = parseInteger(reader.readLine());
                        // =========================================================
                        // NGI 포맷에서는 폴리곤인 경우에도 시작점과 끝점이 다르다.
                        Coordinate[] coordinates = new Coordinate[numofPoints + 1];
                        // =========================================================
                        for (int index = 0; index < numofPoints; index++) {
                            coordinates[index] = parseCoordinate(reader.readLine());
                        }
                        coordinates[numofPoints] = coordinates[0];

                        if (ringIndex == 0) {
                            shell = gf.createLinearRing(coordinates);
                        } else {
                            holes[ringIndex - 1] = gf.createLinearRing(coordinates);
                        }
                    }
                    polygons[partIndex] = gf.createPolygon(shell, holes);
                }

                return gf.createMultiPolygon(polygons);
            }
        } catch (IOException e) {
            LOGGER.log(Level.FINER, e.getMessage(), e);
        }

        return null;
    }

    private boolean nextRecord(BufferedReader reader) {
        try {
            String line = reader.readLine();
            while (line != null) {
                if (line.toUpperCase().indexOf("$RECORD") != -1) {
                    return true;
                }

                if (line.toUpperCase().indexOf("<LAYER_END>") != -1) {
                    return false;
                }
                line = reader.readLine();
            }
        } catch (IOException e) {
            LOGGER.log(Level.FINER, e.getMessage(), e);
        }
        return false;
    }
}
