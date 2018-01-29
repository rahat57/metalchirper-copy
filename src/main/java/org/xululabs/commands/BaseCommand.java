package org.xululabs.commands;

import java.io.BufferedWriter;
import java.io.IOException;

import org.xululabs.commands.TtafResponse;

import twitter4j.Twitter;
import twitter4j.TwitterException;

import com.beust.jcommander.Parameter;
public abstract class BaseCommand {

    @Parameter(names = "--help", help = true, description = "Display help for command")
    private boolean help = false;
    @Parameter(names = "-o", description = "output file name")
    private String outputFile = "output.txt";
//   	public BaseCommand(String path){
//   		setOutputFile(path);
//    }
    public boolean needHelp() {
        return help;
    }

    public void setHelp(boolean help) {
        this.help = help;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(String outputFile) {
//    	CommandIndexTweets cmandtweets = new CommandIndexTweets();
        this.outputFile = outputFile;
        System.err.println(outputFile);
    }
    public abstract TtafResponse execute() throws TwitterException,ClassNotFoundException,Exception;

    public abstract void write(TtafResponse ttafResponse,BufferedWriter writer) throws IOException,Exception;
   
}
