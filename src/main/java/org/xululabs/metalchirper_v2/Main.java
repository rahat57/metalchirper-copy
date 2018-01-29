package org.xululabs.metalchirper_v2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jline.console.ConsoleReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xululabs.commands.TwitterToolAirflow;

import twitter4j.TwitterException;

import com.beust.jcommander.ParameterException;

/**
 * Hello world!
 *
 */
public class Main 
{
  private static ConsoleReader consoleReader;
  private static Logger log = LogManager.getRootLogger();
    public static void main( String[] args ) throws IOException
    {
	 
    	log.info("Initializing twittertool-v3-commandLine");
    	
	         launchCmd(args);
        
    }
 
    
    /**
     * launch tsak in commandline mode
     * 
     * @param args
     * @throws IOException
     */
    public static void launchCmd(String[] args) throws IOException{
        consoleReader = new ConsoleReader();
        TwitterToolAirflow ttaf = new TwitterToolAirflow();
     
        try {

            
            ttaf.executeCommand(args).write();
        }    catch (ParameterException ex) {
       	 System.err.println(ex.getLocalizedMessage());
            log.error(ex.getMessage());
        } catch (TwitterException ex) {
       	 System.err.println(ex.getLocalizedMessage());
            log.error(ex.getMessage());
        } catch (Exception ex) {
       	 System.err.println(ex.getLocalizedMessage());
            log.error(ex.getMessage());
        } finally {
            consoleReader.flush();
       }
    }
    /**
     * parse command.
     * 
     * @return String[] parsed command
     * @throws IOException
     */
    public static String[] getCommandLineArguments() throws IOException {
        String commandLine = consoleReader.readLine("ttaf> ");
        if (commandLine.trim().equals("")) {
            return null;
        }
        if (commandLine.trim().equals("exit")) {
            consoleReader.println("Good Bye...");
            consoleReader.flush();
            System.exit(0);
        }
        List<String> cmdArgs = new ArrayList<String>();
        Pattern regex = Pattern.compile("[^\\s\"']+|\"[^\"]*\"|'[^']*'");
        Matcher regexMatcher = regex.matcher(commandLine);
        while (regexMatcher.find()) {
            cmdArgs.add(regexMatcher.group());
        }
        return cmdArgs.toArray(new String[cmdArgs.size()]);
    }
}
