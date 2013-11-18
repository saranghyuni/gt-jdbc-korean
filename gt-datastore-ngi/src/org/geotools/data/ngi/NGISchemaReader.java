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
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.util.logging.Logging;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

/**
 * NGI Schema Reader
 * 
 * @author MapPlus, mapplus@gmail.com, http://onspatial.com
 * @since 2012-10-30
 * @see
 * 
 */
public class NGISchemaReader extends AbstractNGIReader {
    protected static final Logger LOGGER = Logging.getLogger(NGISchemaReader.class);

    CoordinateReferenceSystem crs;

    final Map<String, SimpleFeatureType> schemas = new TreeMap<String, SimpleFeatureType>();

    final Map<String, ReferencedEnvelope> bounds = new TreeMap<String, ReferencedEnvelope>();

    final Map<String, Integer> counts = new TreeMap<String, Integer>();

    public Map<String, SimpleFeatureType> getSchemas() {
        return Collections.unmodifiableMap(schemas);
    }

    public Map<String, ReferencedEnvelope> getBounds() {
        return Collections.unmodifiableMap(bounds);
    }

    public Map<String, Integer> getCounts() {
        return Collections.unmodifiableMap(counts);
    }

    public NGISchemaReader(File ngiFile, File ndaFile, Charset charset,
            CoordinateReferenceSystem crs) {
        this.crs = crs;
        this.loadSchemas(ngiFile, ndaFile, charset);
    }

    private void loadSchemas(File ngiFile, File ndaFile, Charset charset) {
        BufferedReader ngiReader = null;
        BufferedReader ndaReader = null;
        try {
            ngiReader = new BufferedReader(new InputStreamReader(new FileInputStream(ngiFile),
                    charset));
            if (ndaFile.exists()) {
                ndaReader = new BufferedReader(new InputStreamReader(new FileInputStream(ndaFile),
                        charset));
            }

            String line = ngiReader.readLine();
            while (line != null) {
                String layerName = seekNextLayer(ngiReader);
                if (layerName != null) {
                    // schema
                    SimpleFeatureType schema = createSchema(ngiReader, ndaReader, layerName);
                    if (schema != null) {
                        schemas.put(layerName, schema);
                    }

                    // extent
                    ReferencedEnvelope extent = getBounds(ngiReader);
                    if (extent == null) {
                        extent = new ReferencedEnvelope(crs);
                    }
                    bounds.put(layerName, extent);

                    // feature count
                    if (ndaReader != null) {
                        counts.put(layerName, Integer.valueOf(getCount(ndaReader)));
                    } else {
                        counts.put(layerName, Integer.valueOf(-1));
                    }
                }
                line = ngiReader.readLine();
            }
        } catch (IOException e) {
            LOGGER.log(Level.FINER, e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(ngiReader);
            IOUtils.closeQuietly(ndaReader);
        }
    }

    private SimpleFeatureType createSchema(BufferedReader ngiReader, BufferedReader ndaReader,
            String typeName) {
        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName(typeName);
        builder.setCRS(crs);

        Class<?> geometryBinding = getGeometryType(ngiReader);
        if (geometryBinding != null) {
            if (geometryBinding.isAssignableFrom(Polygon.class)) {
                geometryBinding = MultiPolygon.class;
            } else if (geometryBinding.isAssignableFrom(LineString.class)) {
                geometryBinding = MultiLineString.class;
            }

            builder.add("the_geom", geometryBinding, crs);

            if (ndaReader == null) {
                return builder.buildFeatureType();
            }

            // seek attributes
            try {
                String layerName = seekLayer(ndaReader, typeName);
                boolean hasAttr = seekPosition(ndaReader, "$ASPATIAL_FIELD_DEF");

                if (layerName != null && hasAttr) {
                    String line = ndaReader.readLine();
                    while (line != null) {
                        if (line.toUpperCase().startsWith("ATTRIB")) {
                            // ATTRIB("CODENAME",STRING, 20, 0, FALSE)
                            // ATTRIB(field_name, type, size, decimal, unique)
                            line = line.substring(7, line.length() - 1);
                            String[] values = line.split(",", 5);
                            if (values.length == 5) {
                                String propertyName = values[0]
                                        .substring(1, values[0].length() - 1);
                                final String type = values[1].trim();
                                if (type.toUpperCase().contains("STRING")) {
                                    int length = Integer.parseInt(values[2].trim());
                                    builder.length(length).add(propertyName, String.class);
                                } else if (type.toUpperCase().contains("DATE")) {
                                    builder.length(20).add(propertyName, String.class);
                                } else {
                                    int decimal = Integer.parseInt(values[3].trim());
                                    if (decimal == 0) {
                                        builder.add(propertyName, Integer.class);
                                    } else {
                                        builder.add(propertyName, Double.class);
                                    }
                                }
                            }
                        } else if (line.equalsIgnoreCase("<END>")) {
                            break;
                        }
                        line = ndaReader.readLine();
                    }
                }
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, e.getMessage(), e);
            }
        } else {
            System.out.println(typeName + " has null properties");
        }

        return builder.buildFeatureType();
    }

    private String seekNextLayer(BufferedReader reader) {
        try {
            String line = reader.readLine();
            while (line != null) {
                if (line.equalsIgnoreCase("$LAYER_NAME")) {
                    String layerName = reader.readLine(); // "건물" => 건물
                    return layerName.substring(1, layerName.length() - 1);
                }
                line = reader.readLine();
            }
        } catch (IOException e) {
            LOGGER.log(Level.FINER, e.getMessage(), e);
        }
        return null;
    }

    private int getCount(BufferedReader reader) {
        int featureCount = 0;
        try {
            String line = reader.readLine();
            while (line != null) {
                if (line.toUpperCase().startsWith("$RECORD")) {
                    featureCount++;
                } else if (line.equalsIgnoreCase("<END>")) {
                    break;
                }
                line = reader.readLine();
            }
        } catch (IOException e) {
            LOGGER.log(Level.FINER, e.getMessage(), e);
        }

        return featureCount;
    }

    private ReferencedEnvelope getBounds(BufferedReader reader) {
        try {
            String line = reader.readLine();
            while (line != null) {
                if (line.toUpperCase().startsWith("BOUND(")) {
                    // BOUND(150609.210000, 203279.010000, 152265.620000, 205171.560000)
                    line = line.substring(6, line.length() - 1);
                    String[] coordinates = line.split(",", 4);
                    double x1 = parseDouble(coordinates[0]);
                    double y1 = parseDouble(coordinates[1]);
                    double x2 = parseDouble(coordinates[2]);
                    double y2 = parseDouble(coordinates[3]);

                    return new ReferencedEnvelope(x1, x2, y1, y2, crs);
                }
                line = reader.readLine();
            }
        } catch (IOException e) {
            LOGGER.log(Level.FINER, e.getMessage(), e);
        }
        return null;
    }

    private Class<?> getGeometryType(BufferedReader reader) {
        try {
            String line = reader.readLine();
            while (line != null) {
                if (line.trim().equalsIgnoreCase("$GEOMETRIC_METADATA")) {
                    String shapetypelist = reader.readLine().toUpperCase();
                    shapetypelist = shapetypelist.substring(5, shapetypelist.length() - 1);

                    // MASK(LINESTRING,POLYGON)건물 MASK(LINESTRING,TEXT)도로, MASK(LINESTRING)
                    int pos = shapetypelist.indexOf("POLYGON");
                    String shapetype;
                    if (pos != -1) {
                        shapetype = "POLYGON";
                    } else {
                        pos = shapetypelist.indexOf(",");
                        if (pos > 0) {
                            shapetype = shapetypelist.substring(0, pos);
                        } else {
                            shapetype = shapetypelist;
                        }
                    }

                    if (shapetype.startsWith("TEXT")) {
                        return Point.class;
                    } else if (shapetype.startsWith("POINT")) {
                        return Point.class;
                    } else if (shapetype.startsWith("MULTIPOINT")) {
                        return Point.class;
                    } else if (shapetype.startsWith("LINESTRING")) {
                        return LineString.class;
                    } else if (shapetype.startsWith("MULTILINESTRING")) {
                        return LineString.class;
                    } else if (shapetype.startsWith("MULTILINE")) {
                        return LineString.class;
                    } else if (shapetype.startsWith("NETWORKCHAIN")) {
                        return LineString.class;
                    } else if (shapetype.startsWith("NETWORK CHAIN")) {
                        return LineString.class;
                    } else if (shapetype.startsWith("POLYGON")) {
                        return Polygon.class;
                    } else if (shapetype.startsWith("MULTIPOLYGON")) {
                        return Polygon.class;
                    }
                }
                line = reader.readLine();
            }
        } catch (IOException e) {
            LOGGER.log(Level.FINER, e.getMessage(), e);
        }
        return null;
    }
}
