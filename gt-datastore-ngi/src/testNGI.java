import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.data.Query;
import org.geotools.data.ngi.NGIDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

public class testNGI {

    public static void main(String[] args) throws IOException {
        String ngiFile = "C:/Spatial_Program/ArcGIS_Extension/NGI_SampleDataset/ASC5/35610069.NGI";

        Map<String, Serializable> params = new HashMap<String, Serializable>();
        // 1. native
        params.put(NGIDataStoreFactory.PARAM_FILE.key, DataUtilities.fileToURL(new File(ngiFile)));
        params.put(NGIDataStoreFactory.PARAM_SRS.key, "EPSG:2097");
        params.put(NGIDataStoreFactory.PARAM_CHARSET.key, "EUC-KR");
        // 2.
        params.put("url", DataUtilities.fileToURL(new File(ngiFile)));
        params.put("srs", "EPSG:2097");
        params.put("charset", "EUC-KR");

        // 1.
        NGIDataStoreFactory factory = new NGIDataStoreFactory();
        DataStore dataStore = factory.createDataStore(params);

        List<Name> typeNames = dataStore.getNames();
        for (Name typeName : typeNames) {
            SimpleFeatureSource sfs = dataStore.getFeatureSource(typeName);

            printFeatureSource(sfs);
        }

        System.out.println("완료");
    }

    static void printFeatureSource(SimpleFeatureSource sfs) throws IOException {
        SimpleFeatureType featureType = sfs.getSchema();
        System.out.println(featureType);

        ReferencedEnvelope bounds = sfs.getBounds();
        System.out.println(bounds);

        int featureCount = sfs.getCount(Query.ALL);
        System.out.println(featureCount);

        ReferencedEnvelope unionEnv = null;
        CoordinateReferenceSystem crs = sfs.getSchema().getCoordinateReferenceSystem();

        SimpleFeatureIterator featureIter = null;
        try {
            featureIter = sfs.getFeatures().features();
            while (featureIter.hasNext()) {
                SimpleFeature feature = featureIter.next();
                Geometry geometry = (Geometry) feature.getDefaultGeometry();
                if (geometry == null || geometry.isEmpty()) {
                    continue;
                }

                // System.out.println(feature);

                Envelope curEnv = geometry.getEnvelopeInternal();
                if (unionEnv == null) {
                    unionEnv = new ReferencedEnvelope(curEnv, crs);
                } else {
                    unionEnv.expandToInclude(curEnv);
                }
            }
        } finally {
            featureIter.close();
        }

        System.out.println(unionEnv);
    }

}
