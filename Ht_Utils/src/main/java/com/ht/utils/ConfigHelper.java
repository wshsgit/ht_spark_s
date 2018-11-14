package com.ht.utils;

import java.io.*;
import java.util.Properties;
/**
 * 读取配置文件类
 * @author penghuaiyi
 * @date  2008-8-20
 */
public class ConfigHelper {
    private static final String CONFIG_FILE="../application.properties";
    private static Properties prop;
    private static File mFile;

    static{
        System.out.println("初始化配置文件开始 ");
        prop = new Properties();

        try{
            InputStream in = ConfigHelper.class.getResourceAsStream("/common.properties");
            prop.load(in);
        }catch(IOException e){
            System.out.println (e.getMessage());
            System.out.println ("config file not exist");
        }
    }

    /**
     * 根据key获取属性培植文件中对应的value
     * @param key
     * @return
     */
    public static String getProperty(String key){
        String value = prop.getProperty(key);
        try{
            value = new String(value.getBytes("ISO-8859-1"),"GBK");
        }catch(UnsupportedEncodingException e){
            System.out.println (e.getMessage());
        }
        return value;
    }

    /**
     * 得到resource文件中的属性，将根据字符串数组依次将字符串中的“${0}”、“${1}”...替换<br>
     * 如：aaa=c${0}d${1}efg<br>
     * getProerty("aaa",["k","l"])==ckdlefg
     * @param key
     * @param values
     * @return
     */
    public static String getProperty(String key,String[] values){
        String value = prop.getProperty(key);
        try{
            value = new String(value.getBytes("ISO-8859-1"),"GBK");
        }catch(UnsupportedEncodingException e){
            System.out.println (e.getMessage());
        }
        if(value!=null){
            for(int i=0;i<values.length;i++){
                value=value.replaceFirst("\\$\\{"+i+"\\}", values[i]);
            }
        }

        return value;
    }


    public static void main(String args[]){
        String values[]={"penghuaiyi","dsdsdgsgd"};
        String msg=ConfigHelper.getProperty("zx.log.circuit.newCircuit",values);
        System.out.println(msg);
    }
}