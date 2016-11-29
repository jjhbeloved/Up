package org.humbird.lucene.util;

import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.*;

/**
 * Created by david on 16/11/18.
 */
public class FileUtils {

    public static String tikaFileToStr(File file) throws IOException, TikaException {
        Tika tika = new Tika();
        return tika.parseToString(file);
    }

    public static String fileToStr(File file) {
        Parser parser = new AutoDetectParser();
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(file);
            ContentHandler content = new BodyContentHandler();
            ParseContext parseContext = new ParseContext();
            parseContext.set(Parser.class, parser);
            Metadata metadata = new Metadata();
            parser.parse(inputStream, content, metadata, parseContext);
            return content.toString();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (TikaException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (inputStream!=null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                }
            }
        }
        return null;
    }
}
