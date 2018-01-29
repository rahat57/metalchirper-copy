package org.xululabs.commands;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reflections.Reflections;

import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

/**
 * TwittertoolAirflow is A simple command line utility that allows a user to
 * interact with Twitter's public API. it is very simple, straight forward, easy
 * to use and flexible API.
 * 
 * @author org.projectspinoza
 * @version v1.0.0
 */
public class TwitterToolAirflow{
  
    private static final String TSAK_CORE_COMMANDS_PACKAGE = "org.xululabs.commands";
    private static Logger log = LogManager.getRootLogger();
    private JCommander rootCommander;
    private TtafResponse ttafResponse;
    private List<Class<?>> registeredCommands = new ArrayList<Class<?>>();
    
    /**
     * Instantiates TwitterToolAirflow
     * 
     */
    public TwitterToolAirflow(){
        this(null);
    }
    
    /**
     * Instantiates TwitterToolAirflow
     * 
     * @param twitter
     */
    public TwitterToolAirflow(Twitter twitter){
        this(null, twitter);
    }
  
    /**
     * Instantiates TwitterToolAirflow
     * 
     * @param rootCommander
     * @param twitter
     */
    public TwitterToolAirflow(JCommander rootCommander, Twitter twitter){
        try{
            init(rootCommander);
        }catch(TwitterException tex){
            log.error("Twitter authorization Exception: {}", tex.toString());
        }
    }
    
    /**
     * Initializes TwitterToolAirflow
     * 
     * @param rootCommander
     * @param twitter
     * @throws TwitterException
     */
    private void init(JCommander rootCommander) throws TwitterException{
        if(rootCommander == null){
            rootCommander = new JCommander();
        }
        this.rootCommander = rootCommander;
        registerCommands(TSAK_CORE_COMMANDS_PACKAGE);
    }
    
    
    
    /**
     * register all command classes in the given package, e.g. the classes which extends BaseCommand
     * 
     * @param packageName
     */
    public void registerCommands(String packageName){
        Reflections reflections = new Reflections(packageName);
        Set<Class<? extends BaseCommand>> tsakCommandSet = reflections.getSubTypesOf(BaseCommand.class);
        for (Class<?> clazz : tsakCommandSet) {
            registerCommand(clazz);
        }
    }
    
    /**
     * register the given class as a command class
     * 
     * @param clazz
     */
    public void registerCommand(Class<?> clazz){
        if(BaseCommand.class.isAssignableFrom(clazz) && !registeredCommands.contains(clazz)){
            registeredCommands.add(clazz);
        }else{
            log.error("Cannot register {}, either already registered or un-assignable to BaseCommand", clazz);
        }
    }
    
    /**
     * returns all registered commands
     * 
     * @return registeredCommands
     */
    public List<Class<?>> getRegisteredCommands(){
        return Collections.unmodifiableList(registeredCommands);
    }
    
    
    /**
     * returns result e.g. the generated response of the executed command.
     * 
     * @return TtafResponse
     */
    public TtafResponse getResult(){
        return ttafResponse;
    }
    
    /**
     * returns rootCommander.
     * 
     * @return JCommander
     */
    public JCommander getRootCommander(){
        return rootCommander;
    }
    
    /**
     * returns the instance of the provided command
     * 
     * @param String parsedCommand
     * @return BaseCommand
     */
    public BaseCommand getActiveCommand() {
        String parsedCommand = rootCommander.getCommands().get("ttaf").getParsedCommand();
        return (BaseCommand) rootCommander.getCommands().get("ttaf").getCommands().get(parsedCommand).getObjects().get(0);
    }
    

    
    /**
     * activates all registered commands
     * 
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    private void activateCommands() throws InstantiationException, IllegalAccessException{
        for(Class<?> clazz : registeredCommands){
            rootCommander.getCommands().get("ttaf").addCommand(clazz.newInstance());
        }
    }
    
    
    /**
     * executes the provided command
     * 
     * @param args
     * @return TwitterToolAirflow
     * @throws Exception 
     * @throws ClassNotFoundException 
     */
    public TwitterToolAirflow executeCommand(String[] args) throws ClassNotFoundException, Exception {
        ttafResponse = null;
        if (args == null) {
            log.info("Need help?? run > ttaf <commandName> --help");
            return this;
        }
      
        rootCommander.addCommand("ttaf",new TtafCommand());
        activateCommands();
        rootCommander.parse(args);
        BaseCommand baseCommand = getActiveCommand();    
        if (baseCommand.needHelp()) {
            JCommander tsakCommander = rootCommander.getCommands().get("ttaf");  
            tsakCommander.usage(tsakCommander.getParsedCommand());
            return this;
        }
        
        ttafResponse = baseCommand.execute();
    
        return this;
    }
   
    
    /**
     * writes the result (generated data of the executed command) to the output
     * file.
     * 
     * @return TwitterToolAirflow
     */
   public TwitterToolAirflow write() throws Exception {
        if (ttafResponse == null) {
            return this;
        }
        BufferedWriter bufferedWriter = null;
        
        try {
            BaseCommand baseCommand = getActiveCommand();
            bufferedWriter = new BufferedWriter(new FileWriter(new File(baseCommand.getOutputFile())));
            baseCommand.write(ttafResponse,bufferedWriter);
            bufferedWriter.close();
        } catch (Exception ex) {
            log.debug(ex.getMessage());
        } 
        return this;
    }
   
   
}
