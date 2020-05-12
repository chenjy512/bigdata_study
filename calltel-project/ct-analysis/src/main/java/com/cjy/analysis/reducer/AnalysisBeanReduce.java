package com.cjy.analysis.reducer;


import com.cjy.analysis.kvbean.AnalysisKey;
import com.cjy.analysis.kvbean.AnalysisValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class AnalysisBeanReduce extends Reducer<AnalysisKey, AnalysisValue,AnalysisKey, AnalysisValue>{

    @Override
    protected void reduce(AnalysisKey key, Iterable<AnalysisValue> values, Context context) throws IOException, InterruptedException {

        int sumcall = 0;
        int sumduration = 0;

        Iterator<AnalysisValue> iterator = values.iterator();
        while (iterator.hasNext()){
            AnalysisValue next = iterator.next();
            sumcall += next.getSumCall();
            sumduration += next.getSumDuration();
        }
        context.write(key,new AnalysisValue(sumcall,sumduration));
    }
}
