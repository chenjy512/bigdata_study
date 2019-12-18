package com.cjy.analysis;

import com.cjy.analysis.tool.AnalysisTextTool;
import org.apache.hadoop.util.ToolRunner;

public class BootStrap {
    public static void main(String[] args) throws Exception {
         int result = ToolRunner.run( new AnalysisTextTool(), args );
    }
}
