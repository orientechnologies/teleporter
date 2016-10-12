package com.orientechnologies.teleporter.util;

import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Arrays;
import java.util.List;

/**
 * Created by gabriele on 05/10/16.
 */
public class ODocumentComparator {
    private static int invocation = 0;

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
                if(!areListEquals(doc1CurrValues, doc2CurrValues)) {
                    return false;
                }
            }

            else if( !(doc1CurrValue instanceof ODocument) && (doc2CurrValue instanceof ODocument) ||
                    (doc1CurrValue instanceof ODocument) && !(doc2CurrValue instanceof ODocument)   ) {
                return false;
            }
            else if(!(doc1CurrValue instanceof ODocument) && !(doc2CurrValue instanceof ODocument)) {
                if(!doc1CurrValue.equals(doc2CurrValue)) {  //???? primitives ????
                    return false;
                }
            }
            else if(doc1CurrValue instanceof ODocument && doc2CurrValue instanceof ODocument) {
                if(!areEquals((ODocument) doc1CurrValue, (ODocument) doc2CurrValue)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean areListEquals(List<ODocument> doc1CurrValues, List<ODocument> doc2CurrValues) {

        if(doc1CurrValues.size() != doc2CurrValues.size()) {
            return false;
        }

        for(Object currentDocFromList1: doc1CurrValues) {
            boolean docPresentInSecondList = false;

            for(Object currentDocFromList2: doc2CurrValues) {
                if( !(currentDocFromList1 instanceof ODocument) && (currentDocFromList2 instanceof ODocument) ||
                        (currentDocFromList1 instanceof ODocument) && !(currentDocFromList2 instanceof ODocument)   ) {
                    continue;
                }
                if(!(currentDocFromList1 instanceof ODocument) && !(currentDocFromList2 instanceof ODocument)) {
                    if(currentDocFromList1.equals(currentDocFromList2)) {  //???? primitives ????
                        docPresentInSecondList =  true;
                        break;
                    }
                }
                if(currentDocFromList1 instanceof ODocument && currentDocFromList2 instanceof ODocument) {
                    if(areEquals((ODocument) currentDocFromList1, (ODocument) currentDocFromList2)) {
                        docPresentInSecondList =  true;
                        break;
                    }
                }
            }

            if(!docPresentInSecondList) {
                return false;
            }
        }

        return true;
    }
}
