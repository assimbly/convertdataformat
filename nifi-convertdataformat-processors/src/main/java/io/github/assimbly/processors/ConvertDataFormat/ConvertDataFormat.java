/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.assimbly.processors.ConvertDataFormat;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;

import org.assimbly.docconverter.DocConverter;


@Tags({"XML, JSON, CSV, YAML, DOCUMENT, DATAFORMAT"})
@CapabilityDescription("Converts the data format between structured documents")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ConvertDataFormat extends AbstractProcessor {

    public static final PropertyDescriptor INPUT_DOCUMENT = new PropertyDescriptor
            .Builder().name("INPUT_DOCUMENT")
            .displayName("INPUT DOCUMENT")
            .description("Data Format of the input document")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("XML")
            .allowableValues("CSV","JSON","XML","YAML")
            .build();

    public static final PropertyDescriptor OUTPUT_DOCUMENT = new PropertyDescriptor
            .Builder().name("OUTPUT_DOCUMENT")
            .displayName("OUTPUT DOCUMENT")
            .description("Data Format of the output document")
            .required(true)
            .defaultValue("JSON")
            .allowableValues("CSV","JSON","XML","YAML")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();    

    public static final PropertyDescriptor SET_EXTENSION = new PropertyDescriptor
            .Builder().name("SET_EXTENSION")
            .displayName("SET FILE EXTENSION")
            .description("Sets file extension to data format of the output document")
            .required(true)
            .defaultValue("FALSE")
            .allowableValues("TRUE","FALSE")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Succes relationship")
            .build();
    
    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private String input;
    private String output; 

    
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(INPUT_DOCUMENT);
        descriptors.add(OUTPUT_DOCUMENT);
        descriptors.add(SET_EXTENSION);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        
    	FlowFile flowfile = session.get();
  
        if ( flowfile == null ) {
            return;
        }
                
        final String inputDocumentProperty = context.getProperty(INPUT_DOCUMENT).getValue();
    	final String outputDocumentProperty = context.getProperty(OUTPUT_DOCUMENT).getValue();
    	final String setExtensionProperty = context.getProperty(SET_EXTENSION).getValue();
    	        
        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try{
                	
                	
                	String converter = inputDocumentProperty + "2" + outputDocumentProperty;
                	input = DocConverter.convertStreamToString(in);
                	
                     switch(converter) 
                     { 
                         case "CSV2JSON": 
                        	 output = DocConverter.convertCsvToJson(input);
                             break; 
                         case "CSV2XML": 
                        	 output = DocConverter.convertCsvToXml(input);
                             break; 
                         case "CSV2YAML": 
                        	 output = DocConverter.convertCsvToYaml(input);
                             break; 
                         case "JSON2CSV": 
                        	 output = DocConverter.convertJsonToCsv(input);
                             break; 
                         case "JSON2XML": 
                        	 output = DocConverter.convertJsonToXml(input);
                             break; 
                         case "JSON2YAML": 
                        	 output = DocConverter.convertJsonToYaml(input);
                             break;
                         case "XML2CSV": 
                        	 output = DocConverter.convertXmlToCsv(input); 
                             break; 
                         case "XML2JSON": 
                        	 output = DocConverter.convertXmlToJson(input); 
                             break; 
                         case "XML2YAML": 
                        	 output = DocConverter.convertXmlToYaml(input); 
                             break;                             
                         case "YAML2CSV": 
                        	 output = DocConverter.convertYamlToCsv(input);
                             break; 
                         case "YAML2JSON": 
                        	 output = DocConverter.convertYamlToJson(input);
                             break; 
                         case "YAML2XML": 
                        	 output = DocConverter.convertYamlToXml(input);
                             break;                         
                         default: 
                             output = input; 
                     }  
                }catch(Exception ex){
                    ex.printStackTrace();
                    getLogger().error("Failed to read/convert input document.");
                }
            }
        });

        String currentFileName = flowfile.getAttribute("filename");
        
        if(setExtensionProperty.equals("TRUE") && !currentFileName.isEmpty()){
        	String newFileName = renameFileExtension(currentFileName,outputDocumentProperty.toLowerCase());
            flowfile = session.putAttribute(flowfile, "filename", newFileName);
        }

        // To write the results back out to flow file
        flowfile = session.write(flowfile, new OutputStreamCallback() {

            @Override
            public void process(OutputStream out) throws IOException {
                out.write(output.getBytes());
            }
        });

        session.transfer(flowfile, SUCCESS);
    }
    
    public String renameFileExtension(String source, String newExtension)  
	{  
	  String target;  
	  String currentExtension = getFileExtension(source);  
	  
	  if (currentExtension.equals("")){  
	     target = source + "." + newExtension;  
	  }  
	  else {  
	     target = source.replaceAll(currentExtension, newExtension);  
	  }  
	  return target;  
	}  
	  
	public static String getFileExtension(String f) {  
	  String ext = "";  
	  int i = f.lastIndexOf('.');  
	  if (i > 0 &&  i < f.length() - 1) {  
	     ext = f.substring(i + 1).toLowerCase();  
	  }  
	  return ext;  
	} 
    
}
