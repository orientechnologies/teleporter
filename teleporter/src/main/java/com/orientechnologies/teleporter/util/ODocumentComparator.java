package com.orientechnologies.teleporter.util;

import com.oracle.javafx.jmx.json.JSONDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Arrays;
import java.util.List;

/**
 * Created by gabriele on 05/10/16.
 */
public class ODocumentComparator {

    public static boolean areEquals(ODocument doc1, ODocument doc2) {

        //JSONDocument doc1 = new JSONDocument()

        List<String> doc1FieldNames = Arrays.asList(doc1.fieldNames());
        List<String> doc2FieldNames = Arrays.asList(doc2.fieldNames());

        if(doc1FieldNames.size() != doc2FieldNames.size()) {
            return false;
        }

        for(String fieldName: doc1FieldNames) {
            if(!doc2FieldNames.contains(fieldName)) {
                return false;
            }
            Object doc1CurrValue = doc1.field(fieldName);
            Object doc2CurrValue = doc2.field(fieldName);

            if(doc1CurrValue instanceof List && doc2CurrValue instanceof List ) {
                List<ODocument> doc1CurrValues = doc1.field(fieldName);
                List<ODocument> doc2CurrValues = doc2.field(fieldName);
                areListEquals(doc1CurrValues, doc2CurrValues);
            }

            if( !(doc1CurrValue instanceof ODocument) && (doc2CurrValue instanceof ODocument) ||
                    (doc1CurrValue instanceof ODocument) && !(doc2CurrValue instanceof ODocument)   ) {
                return false;
            }

            if(!(doc1CurrValue instanceof ODocument) && !(doc2CurrValue instanceof ODocument)) {
                if(!doc1CurrValue.equals(doc1CurrValue)) {
                    return false;
                }
            }

            if(doc1CurrValue instanceof ODocument && doc2CurrValue instanceof ODocument) {
                if(areEquals(doc1, doc2)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static void areListEquals(List<ODocument> doc1CurrValues, List<ODocument> doc2CurrValues) {

    }
}
