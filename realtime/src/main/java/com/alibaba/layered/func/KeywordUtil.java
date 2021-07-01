package com.alibaba.layered.func;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/7/1 10:37
 */
public class KeywordUtil {

    //拆分单词方法，参数str, 调用 IKSegmenter(sr, true)， 遍历后放入集合中 ; 方法名 analyze ， 返回值 List<String>
    public static List<String> analyze(String str){

        List<String> list = new ArrayList<String>();
        StringReader readStr = new StringReader(str);
        //传入参数： Reader input
        IKSegmenter ikSegmenter = new IKSegmenter(readStr, true);
        Lexeme lexeme = null;

        //避免编译错误
        while(true){
            try {
                //获取一个单词
                if((lexeme = ikSegmenter.next())!=null){
                    String word = lexeme.getLexemeText();
                    list.add(word);
                }else{
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return list;
    }




    //测试分词方法
    public static void main(String[] args) {
        // 智能分词：这个自定义的分词，可以将单词准确的分处理
        String str = "I'm totally in love with this zero waste romper for my littlest one! This one's" +
                " not one of my makes (my sewing skills leave a little to be desired when it comes to clothing) " +
                "it's a completely bespoke make from made using all the left over bits of fabric :)";
        System.out.println(analyze(str));

    }

}
