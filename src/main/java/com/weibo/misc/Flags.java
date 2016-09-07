package com.weibo.misc;

import org.apache.commons.cli.*;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

/**
 * 工具类：参数解析、检查与输出帮助信息
 * 注：
 * 1. 采用标准可移植接口方式，所有参数均以字符串对待
 * 2. Options只用来约束检查参数和暂时存放，实际参数存于optionValues中
 * Created by yuanye8 on 16/9/2.
 */
public class Flags {
    protected Options options;
    protected CommandLine cmdLine;
    protected Hashtable<String, String> optionValues;
    public static final String FLAGS_PREFIX = "flags_";

    public Flags() {
        this.options = new Options();
        this.optionValues = new Hashtable<String, String>();
    }

    /**
     * 添加必须参数，无描述信息
     *
     * @param name
     */
    public void add(String name) {
        add(name, "<no description>");
    }

    /**
     * 添加必须参数，带描述信息
     *
     * @param name
     * @param description
     */
    public void add(String name, String description) {
        add(name, true, true, description);
    }

    /**
     * @param name
     * @param hasValue    参数是否有值
     * @param required    参数是否必须
     * @param description
     */
    private void add(String name, boolean hasValue, boolean required, String description) {
        Option option = new Option(name, hasValue, description);
        option.setRequired(required);
        this.options.addOption(option);
        this.optionValues.put(name, "");
    }

    /**
     * @param name
     * @param defaultValue 默认值
     * @param description
     */
    public void addWithDefaultValue(String name, String defaultValue, String description) {
        Option option = new Option(name, true, description);
        option.setRequired(false);
        this.options.addOption(option);
        this.optionValues.put(name, defaultValue);
    }

    /**
     * 转换命令行输入参数，并将其存入hashTable中
     * 命令行参数转换由options进行检查，分为必须参数和非必须参数
     *
     * @param args
     * @throws ParseException 上层封装parseAndCheck来处理参数转换异常
     */
    public void parse(String[] args) throws ParseException {
        CommandLineParser parser = new PosixParser();
        this.cmdLine = parser.parse(this.options, args);
        for (String optionName : this.optionValues.keySet()) {
            if (optionName != null) {
                String value = this.cmdLine.getOptionValue(optionName);
                if (value != null) {
                    this.optionValues.put(optionName, value);
                }
            }
        }
    }

    /**
     * 参数检查，异常处理
     *
     * @param args
     */
    public void parseAndCheck(String[] args) {
        try {
            parse(args);
        } catch (ParseException e) {
            System.out.println(e);
            printHelp();
            System.exit(0);
        }
    }

    /**
     * 使用工具类进行参数帮助输出
     */
    private void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("hadoop-lda-model-jar", this.options);
    }

    public int getInt(String name) {
        return Integer.parseInt((String) this.optionValues.get(name));
    }

    public double getDouble(String name) {
        return Double.parseDouble((String) this.optionValues.get(name));
    }

    public String getString(String name) {
        return (String) this.optionValues.get(name);
    }

    public boolean getBoolean(String name) {
        return Boolean.parseBoolean((String) this.optionValues.get(name));
    }

    public File getFile(String name) {
        return new File((String) this.optionValues.get(name));
    }

    /**
     * 命令行参数 => MapReudce程序配置
     *
     * @param jobConf
     */
    public void saveToJobConf(JobConf jobConf) {
        for (Map.Entry entry : this.optionValues.entrySet()) {
            jobConf.set(FLAGS_PREFIX + (String) entry.getKey(), (String) entry.getValue());
        }
    }

    /**
     * MapReduce程序配置 => 命令行参数
     *
     * @param job
     */
    public void loadFromJobConf(JobConf job) {
        Iterator<Map.Entry<String, String>> iter = job.iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            if (((String) entry.getKey()).startsWith(FLAGS_PREFIX)) {
                String optionName = ((String) entry.getKey()).substring(FLAGS_PREFIX.length());
                this.optionValues.put(optionName, (String) entry.getValue());
            }
        }
    }

}
