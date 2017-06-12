package com.cbds.readnotstructuredfiles;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.pdfbox.cos.COSDocument;
import org.apache.pdfbox.io.RandomAccessBufferedFileInputStream;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.poi.hslf.usermodel.HSLFShape;
import org.apache.poi.hslf.usermodel.HSLFSlide;
import org.apache.poi.hslf.usermodel.HSLFSlideShow;
import org.apache.poi.hslf.usermodel.HSLFTextShape;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.hwpf.extractor.WordExtractor;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFShape;
import org.apache.poi.xslf.usermodel.XSLFSlide;
import org.apache.poi.xslf.usermodel.XSLFTextShape;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xwpf.extractor.XWPFWordExtractor;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.xml.sax.SAXException;

/*
 * File Extraction Class that provide a way to extract text from not structured files.
 */
public class FileExtraction {
	
	//Method that returns the text from a not structured file as a List of Strings.
	public List<String> fileExtraction(FSDataInputStream fileIn,String ext) throws IOException, Exception{
		//List of String will contain the text from not structured files.
		List<String> contenido = null;
		//Conditional sentence that check extension of the file to extract its text.
		if (fileIn != null) {
		      if(ext.equalsIgnoreCase("pdf")){
					contenido = pdfTransformation(fileIn.getWrappedStream());
		      }else if(ext.equalsIgnoreCase("doc")){
			      contenido = wordDocTransformation(fileIn.getWrappedStream());
		      }else if(ext.equalsIgnoreCase("docx")){
			      contenido = wordDocxTransformation(fileIn.getWrappedStream());
		      }else if(ext.equalsIgnoreCase("xls")){
		    	  contenido = wordXlsTransformation(fileIn.getWrappedStream());
		      }else if(ext.equalsIgnoreCase("xlsx")){
		    	  contenido = wordXlsxTransformation(fileIn.getWrappedStream());
		      }else if(ext.equalsIgnoreCase("ppt")){
		    	  contenido = powerPptTransformation(fileIn.getWrappedStream());
		      }else if(ext.equalsIgnoreCase("pptx")){
		    	  contenido = powerPptxTransformation(fileIn.getWrappedStream());
		      }
		}
		return contenido;
	}
	
	//Method to extract text from a xlsx file.
	public List<String> wordXlsxTransformation(InputStream is) throws IOException, FileNotFoundException, Exception{
		XSSFWorkbook myWorkBook = new XSSFWorkbook (is); 
		// Return first sheet from the XLSX workbook 
		List<String> cadenas = new ArrayList<String>();;
		
		int numberOfSheets = myWorkBook.getNumberOfSheets();
		for(int x=0; x < numberOfSheets; x++){
			XSSFSheet mySheet = myWorkBook.getSheetAt(x); 
			// Get iterator to all the rows in current sheet 
			Iterator<Row> rowIterator = mySheet.iterator(); 
			// Traversing over each row of XLSX file 

			while (rowIterator.hasNext()) { 
				String textoExcel = "";
				Row row = rowIterator.next(); 
				// For each row, iterate through each columns 
				Iterator<Cell> cellIterator = row.cellIterator(); 
				while (cellIterator.hasNext()) { 
					Cell cell = cellIterator.next(); 
					switch (cell.getCellType()) { 
						case Cell.CELL_TYPE_STRING: 
							textoExcel = textoExcel + " " + cell.getStringCellValue();
							break; 
					case Cell.CELL_TYPE_NUMERIC: 
							textoExcel = textoExcel + " " + cell.getNumericCellValue();
							break; 
					case Cell.CELL_TYPE_BOOLEAN: 
							textoExcel = textoExcel + " " + cell.getBooleanCellValue();
							break; 
					default : 
						} 
				} 
			cadenas.add(textoExcel);
		   }
		}
		myWorkBook.close();
		return cadenas;
	}
	
	//Method to extract text from a xls file.
	public List<String> wordXlsTransformation(InputStream is) throws IOException, FileNotFoundException, Exception{
		
		//Get the workbook instance for XLS file 
		HSSFWorkbook workbook = new HSSFWorkbook(is);
		int numberOfSheets = workbook.getNumberOfSheets();
		List<String> cadenas = new ArrayList<String>();

		for(int x=0; x < numberOfSheets; x++){
			//Get first sheet from the workbook
			HSSFSheet sheet = workbook.getSheetAt(x);
			
			//Iterate through each rows from first sheet
			Iterator<Row> rowIterator = sheet.iterator();
			while(rowIterator.hasNext()) {
				Row row = rowIterator.next();
				
				//For each row, iterate through each columns
				Iterator<Cell> cellIterator = row.cellIterator();
				String textoExcel = "";
				while(cellIterator.hasNext()) {
					
					Cell cell = cellIterator.next();
					
					switch(cell.getCellType()) {
						case Cell.CELL_TYPE_BOOLEAN:
							textoExcel = textoExcel + " " + cell.getBooleanCellValue();
							break;
						case Cell.CELL_TYPE_NUMERIC:
							textoExcel = textoExcel + " " + cell.getNumericCellValue();
							break;
						case Cell.CELL_TYPE_STRING:
							textoExcel = textoExcel + " " + cell.getStringCellValue();
							break;
					}
				}
				cadenas.add(textoExcel);
			}
		}
		workbook.close();
	  return cadenas;
	}
	
	//Method to extract text from a pdf file.
	public List<String> pdfTransformation(InputStream inputstream) throws IOException, SAXException, Exception{		   
		  	       
	       PDFParser parser = new PDFParser(new RandomAccessBufferedFileInputStream(inputstream)); 
	       
	       parser.parse();
	       COSDocument cosDoc = parser.getDocument();
	       PDFTextStripper pdfStripper = new PDFTextStripper();
	       PDDocument pdDoc = new PDDocument(cosDoc);
	       int numeroPages = pdDoc.getNumberOfPages();
	       pdfStripper.setStartPage(1);
	       pdfStripper.setEndPage(numeroPages);
	       
	       List<String> s = new ArrayList<String>();
		   s.add(pdfStripper.getText(pdDoc).replace("\n"," " ).replace("\r", " "));
		   pdDoc.close();
		   cosDoc.close();
	      return s;
	      
	}
	
	//Method to extract text from a doc file.
	public List<String> wordDocTransformation(InputStream inputstream) throws IOException, Exception{
		    HWPFDocument document = new HWPFDocument(inputstream);
		    WordExtractor extractor = new WordExtractor(document);
	        String[] fileData = extractor.getParagraphText();
	        String contenido = "";
	        for (int i = 0; i < fileData.length; i++){
	            if (fileData[i] != null)
	           	 contenido += fileData[i];
	        }
		      List<String> s = new ArrayList<String>();
		      s.add(contenido);
		      extractor.close();
		      return s;
	  }
	
	  //Method to extract text from a docx file.
	  public List<String> wordDocxTransformation(InputStream inputstream) throws IOException, Exception{
		      XWPFDocument doc = new XWPFDocument(inputstream);
		      XWPFWordExtractor ex = new XWPFWordExtractor(doc);
		      List<String> s = new ArrayList<String>();
		      s.add(ex.getText());
		      ex.close();
		      return s;
	  }
	  
	  //Method to extract text from a pptx file.
	  public List<String> powerPptxTransformation(InputStream inputstream) throws IOException, Exception{
	        XMLSlideShow ppt    = new XMLSlideShow(inputstream);
	        List<String> s 		= new ArrayList<String>();
	        for (XSLFSlide slide: ppt.getSlides()) {
	        	List<XSLFShape> shapes = slide.getShapes();
	        	for (XSLFShape shape: shapes) {
	        		if (shape instanceof XSLFTextShape) {
	        	        XSLFTextShape textShape = (XSLFTextShape)shape;
	        	        s.add(textShape.getText());
	        		}
	        	}
	        }	
	        ppt.close();
	        return s;
	  }
	  
	  //Method to extract text from a pptx file.
	  public List<String> powerPptTransformation(InputStream inputstream) throws IOException, Exception{
	    	HSLFSlideShow ppt    = new HSLFSlideShow(inputstream);
	        List<String> s 		= new ArrayList<String>();
	        for (HSLFSlide slide: ppt.getSlides()) {
	        	List<HSLFShape> shapes = slide.getShapes();
	        	for (HSLFShape shape: shapes) {
	        		if (shape instanceof HSLFTextShape) {
	        			HSLFTextShape textShape = (HSLFTextShape)shape;
	        	        s.add(textShape.getText());
	        		}
	        	}
	        }	
	        ppt.close();
	        return s;
	  }
}
