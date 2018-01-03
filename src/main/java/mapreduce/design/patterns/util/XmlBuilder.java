package mapreduce.design.patterns.util;

import org.w3c.dom.*;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;

/**
 * Created by Sergiy Dyrda
 * <p>
 * on 14.12.2017.
 */
public class XmlBuilder {

    private DocumentBuilderFactory factory;

    public XmlBuilder(DocumentBuilderFactory factory) {
        this.factory = factory;
    }

    public String nestElementsFromRawData(String parent, List<String> children, String parentName, String childrenName) throws Exception {
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.newDocument();
        Element parentElement = document.createElement(parentName);
        copyAttributesToElement(parent, parentElement);

        for (String child : children) {
            Element childElement = document.createElement(childrenName);
            copyAttributesToElement(child, childElement);
            parentElement.appendChild(childElement);
        }

        document.appendChild(parentElement);
        return transformNodeToString(document);
    }

    public String nestMatureElements(String parent, List<String> children) throws IOException, SAXException, ParserConfigurationException, TransformerException {
        Element parentElement = getXmlElementFromString(parent);
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.newDocument();
        document.adoptNode(parentElement);
        for (String child : children) {
            Element childElem = getXmlElementFromString(child);
            document.adoptNode(childElem);
            parentElement.appendChild(childElem);
        }


        document.appendChild(parentElement);
        return transformNodeToString(document);
    }

    public Element getXmlElementFromString(String xml) throws ParserConfigurationException, IOException, SAXException {
        DocumentBuilder builder = factory.newDocumentBuilder();
        return builder.parse(new InputSource(new StringReader(xml))).getDocumentElement();
    }

    private void copyAttributesToElement(String xmlString, Element element) throws ParserConfigurationException, IOException, SAXException {
        Element xmlElementFromString = getXmlElementFromString(xmlString);
        NamedNodeMap attributes = xmlElementFromString.getAttributes();
        for (int i = 0; i < attributes.getLength(); i++) {
            Attr item = (Attr) attributes.item(i);
            element.setAttribute(item.getName(), item.getValue());
        }
    }

    public String transformNodeToString(Node document) throws TransformerException {
        TransformerFactory factory = TransformerFactory.newInstance();
        Transformer transformer = factory.newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        StringWriter writer = new StringWriter();
        transformer.transform(new DOMSource(document), new StreamResult(writer));

        return writer.toString();
    }
}
