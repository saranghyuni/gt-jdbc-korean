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
 
Histroy
========
author : MapPlus, mapplus@gmail.com, http://onspatial.com
since  : 2012-10-30

Compatibility
==============
 - GeoTools 2.7.x
 - GeoTools 8.x
 - 모두 지원
 
Overview
=========

#. NGI 포맷은 수치지도Ver 2.0의 배포를 위한 국토지리정보원 내부포맷으로 다음과 같은 특징이 있습니다.
   #. 공간데이터와 비공간 데이터의 저장시 파일분리
   #. 공간데이터(*.NGI, *.NBI), 속성데이터(*.NDA, *.NDB) 표현
   #. 공간데이터와 비공간 데이터를 서로 연결할 수 있도록 UID(Record ID) 사용
   #. 수치지도(Ver.2.0)에 적합한 데이터
   #. 위상수준 수용: 네트워크 위상까지 표현
   #. 아스키파일과 바이너리 파일 포맷 제공


Constraints
===========

#. NGI 포맷은 데이터 교환포맷이므로 서비스(GeoServer 등) 데이터로 활용하지는 않습니다.
#. 이 드라이브는 데이터 변환을 위한 용도로만 활용할 수 있습니다.
#. 읽기 전용이며 아스키 파일만 지원합니다.
#. Filter를 이용한 쿼리 미적용
#. MULTILINESTRING, MULTIPOLYGON은 테스트하지 못했습니다.
#. SimpleFeatureSource로 얻을 수 있는 정보는 다음과 같습니다.
   - getSchema()
   - getBounds()
   - getCount(Query.ALL)
   
#. 예제 코드
    String ngiFile = "C:/Spatial_Program/ArcGIS_Extension/NGI_SampleDataset/ASC5/NGI_5000.NGI";
    
    Map<String, Serializable> params = new HashMap<String, Serializable>();
    
    // 1
    params.put(NGIDataStoreFactory.PARAM_FILE.key, DataUtilities.fileToURL(new File(ngiFile)));
    params.put(NGIDataStoreFactory.PARAM_SRS.key, "EPSG:2097");
    params.put(NGIDataStoreFactory.PARAM_CHARSET.key, "EUC-KR");
    
    // or 2
    params.put("url", DataUtilities.fileToURL(new File(ngiFile)));
    params.put("srs", "EPSG:2097");
    params.put("charset", "EUC-KR");
    
    // 1
    NGIDataStoreFactory factory = new NGIDataStoreFactory();
    DataStore dataStore = factory.createDataStore(params);
    
    // or 2
    dataStore = DataStoreFinder.getDataStore(params);
    
    
    List<Name> typeNames = dataStore.getNames();
    for (Name typeName : typeNames) {
        SimpleFeatureSource sfs = dataStore.getFeatureSource(typeName);
        // do something
    }
