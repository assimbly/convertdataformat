# About

ConvertDataFormat is a processor for [Apache Nifi](http://nifi.apache.org/). It converts the dataformat from and to XML, JSON, CSV or YAML. 

It's a generic converter without any options. If you need more specific functionality, check if there is specialized converter.

The processor is based on [DocConverter](https://github.com/assimbly/docconverter)

### Installation

1. [Download](https://github.com/assimbly/convertdataformat/releases) the NAR file.
2. Put the NAR file in the lib directory of Nifi.
3. For older installations of Nifi (before version 1.9) you need to restart.

### Usage

The processor has 3 properties:

* Input document: select the data format of the input document
* Output document: select the data format of the output document
* Set extension: If true and there is a filename attribute then it sets the extension of the output document. 