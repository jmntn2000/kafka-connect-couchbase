package com.couchbase.connect.kafka.util;

import com.couchbase.client.java.Bucket;

import java.io.IOException;
import java.util.regex.Pattern;

public class LocationExtractor {
    DocumentPathExtractor bucketExtractor;
    DocumentPathExtractor scopeExtractor;
    DocumentPathExtractor collectionExtractor;
    boolean useDefaultBucket;

    public LocationExtractor(String locationString){
        String[] parts = locationString.split(Pattern.quote("."));

        int i=0;
        if(parts.length!=3){
            useDefaultBucket=false;
            bucketExtractor = new DocumentPathExtractor(parts[i++],false);
        }else{
            useDefaultBucket=true;
        }
        scopeExtractor = new DocumentPathExtractor(parts[i++],false);
        collectionExtractor = new DocumentPathExtractor(parts[i],false);
    }

    public String getBucket(byte[] json) throws IOException, DocumentPathExtractor.DocumentPathNotFoundException {
        if(useDefaultBucket){
            return null;
        }
        DocumentPathExtractor.DocumentExtraction extraction = bucketExtractor.extractDocumentPath(json);

        return extraction.getPathValue();
    }

    public String getScope(byte[] json) throws IOException, DocumentPathExtractor.DocumentPathNotFoundException {
        DocumentPathExtractor.DocumentExtraction extraction = scopeExtractor.extractDocumentPath(json);

        return extraction.getPathValue();
    }

    public String getCollection(byte[] json) throws IOException, DocumentPathExtractor.DocumentPathNotFoundException {
        DocumentPathExtractor.DocumentExtraction extraction = collectionExtractor.extractDocumentPath(json);

        return extraction.getPathValue();
    }

}
