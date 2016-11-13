package datavirtue;


import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.font.FontRenderContext;
import java.awt.font.LineBreakMeasurer;
import java.awt.font.TextAttribute;
import java.awt.font.TextLayout;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.awt.print.Book;
import java.awt.print.PageFormat;
import java.awt.print.Printable;
import java.awt.print.PrinterJob;
import java.text.AttributedString;
import java.util.Vector;
import java.awt.image.*;
import java.awt.*;
import java.awt.font.*;
import java.awt.print.Paper;



/**
 *
 * @author Data Virtue  - Public Domain - Copyright is hereby relenquished to the public domain.

 Sorry for any spelling mistakes, if you improve this class PLEASE send me the new code!

 seanka@datavirtue.com


 This class simulates an old style (dot matrix, daisy wheel) printer.
 Just feed it the page size and some text: addLine(String), newLine(), formFeed(), and it shoots out perfectly on the paper
 If boolean prompt is set to false no print dialog is shown and printing takes place immediatly.
 This class still needs optimized for memory, you must always step lightly when printing large amount of data/pages.

 This class could stand to be modified to allow greater control and configuration of the rendering page.

 see main() for an example, just compile and run this class to see it in action.  A print dialog will appear.

 */

public class LinePrinter {

    /* instance vars */

    private PageFormat pgFormat = new PageFormat();

    private Book book = new Book();

	private Paper p;


    private boolean prompt = true;

	private int lineSize = 0;
	private int Y_space = 0;

	private java.util.ArrayList lines = new java.util.ArrayList();

	private int W;
	private int H;
	private int usedLines = 0;
	private int availableLines=0;
	private  Font font;


    /** Create a new instance of LinePrinter */

    public LinePrinter(Font fnt, boolean prompt) {

		this.setFont(fnt);


        this.prompt = prompt;


		/* Setup the paper */
        p = new Paper();

        p.setSize(W = 612, H = 792);  //8.5" x 11"

		p.setImageableArea(36, 36, W-36 ,H-36);  //half inch margins

        pgFormat.setPaper(p);


		reconfigurePage();
		/*
		double fh = fontMetrics.getHeight();


        System.out.println("Font Family - "+font.getFamily());
        System.out.println("Font Name - "+font.getFontName());
        System.out.println("Font Logical Name - "+font.getName());
        System.out.println(font.getSize());
        System.out.println("Font metric height: "+fh);
        //System.exit(0);*/



    }

 /** Create a new instance of LinePrinter */

    public LinePrinter(boolean prompt) {

        this.setFont(new Font("courier", Font.PLAIN, 12));


        this.prompt = prompt;


		/* Setup the paper */
        p = new Paper();

        p.setSize(W = 612, H = 792);  //8.5" x 11"

        p.setImageableArea(36, 36, W-36 ,H-36);  //half inch margins

        pgFormat.setPaper(p);


		reconfigurePage();
	
    }
    
    
	private void reconfigurePage() {

		/* calc linesAvailable and other metrics */
		/* get BufferedImage to create a grphics so we can do font calcs before turning the printable loose */
				BufferedImage bimage = new BufferedImage(612-72, 792-72, BufferedImage.TYPE_INT_RGB);

				Graphics2D g2d = bimage.createGraphics();

				g2d.setFont(font);

				FontRenderContext fr = g2d.getFontRenderContext();
				LineMetrics lm = font.getLineMetrics( "HijK", fr );

				lineSize = (int)lm.getHeight();


				//done with g2d
				g2d = null;


		//System.out.println("Line size: "+lineSize);

		Y_space = (int)p.getImageableHeight();

				//System.out.println("Available Y_Space: " + Y_space);

		        availableLines = (Y_space - lineSize) / lineSize;  //the PROPER formula for calculating available lines on a page using the specified font

				//System.out.println("Available lines calc: " + availableLines);



	}

    /** Adds the text Strings to the page.
    If a line goes over the page a new page is created and "printing" continues on the next page. */
    public void addLines (String [] text) {

        //labels.append(new DymoLabel(text), pgFormat);
		for (int i = 0; i < text.length; i++){

			if (usedLines+1 > availableLines) formFeed();

			lines.add(text[i]);
			usedLines++;

		}


    }


    /** Adds a line of text to the page.
    If a line goes over the page a new page is created and "printing" continues on the next page.
    You can create more methods to accept Vectors/Genrics and what not.
    Real new lines inside the string are ignored when rendering.
    This can be chaged by modifying this method to scan the string for the amount of newlines and then calling the newLine() method for each one.*/
    public void addLine (String line) {

        //labels.append(p, pgFormat);
		if (usedLines+1 > availableLines) formFeed();
		lines.add(line);
		usedLines++;



    }


	/**Adds a "new line" (blank line). */
	public void newLine() {

		addLine("");


	}
	/** Manually starts a new page. */
    public void formFeed() {

		book.append(new LinePrinterPage(lines, font), pgFormat);

		lines = new java.util.ArrayList();

		usedLines = 0;

	}


	/**Starts a new "page" with a different font */
	public void formFeed(Font f) {



			book.append(new LinePrinterPage(lines, font), pgFormat);

			font = f;

			lines = new java.util.ArrayList();

			usedLines = 0;

			reconfigurePage();



	}


	/** The whole document will be rendered in this font until forFeed(Font f) is called at least.  */
    private void setFont(Font f){

		font = f;

	}

    /** Set the orientation before calling formFeed() or go(). */
    public void setOrientation(String orient) {

		if (orient.equalsIgnoreCase("portrait")){

			//pgFormat.setOrientation(PageFormat.PORTRAIT);
                    p.setSize(W = 612, H = 792);  //8.5" x 11"

                    p.setImageableArea(36, 36, W-36 ,H-36);  //half inch margins

                    pgFormat.setPaper(p);

                    reconfigurePage();

		}else {

			if (orient.equalsIgnoreCase("landscape")){

				//pgFormat.setOrientation(PageFormat.LANDSCAPE);
                                p.setSize(W = 792, H = 612);  //11" x 8.5"

                                p.setImageableArea(36, 36, W-36 ,H-36);  //half inch margins

                                pgFormat.setPaper(p);
                                reconfigurePage();
			}

		}


	}


    
    

    public void go () {

        /* Print the Book */
        PrinterJob printJob = PrinterJob.getPrinterJob();

        printJob.setPageable(book);  //contains all pgFormats

        //new PreviewDialog (null, true,book);
        
        
        boolean doJob = true;

        if (prompt){

            doJob = printJob.printDialog();

        }

        if (doJob) {

            try {


                printJob.print();

             }catch (Exception PrintException) {

                PrintException.printStackTrace();

             }

        }


    }


    public static void main(String[] args) {


        //                                         Select Printer
        LinePrinter dp = new LinePrinter(new Font("roman", Font.PLAIN, 10), true);  //Regular DYMO ~ Co Star LabelWriter Labels '30252'

        dp.addLines(new String [] {"", "Data Virtue", "5244 Leininger-Haigh Road", "Hillsboro, OH  45133"});

        dp.formFeed();

        for (int i = 1; i < 100; i++) {

			dp.addLines(new String [] {"Sean Anderson", "", "5244 Leininger-Haigh Road", "Hillsboro, OH  45133"});
			//dp.formFeed(); //put each address on a new page
		}

        dp.go();





	}

}//END DYMOPRINTER CLASS



/* When printing the various strings for the page new lines within the string are ignored  */
class LinePrinterPage implements Printable {  //this can access the vars in

    /** Creates a new instance of DymoLabel */
    public LinePrinterPage(java.util.ArrayList lines, Font f) {

        this.lines = lines;
        font = f;


    }

    private java.util.ArrayList lines;
	private Font font;

    public int print(Graphics g, PageFormat pageFormat, int page) {

      //--- Create the Graphics2D object
      Graphics2D g2d = (Graphics2D) g;


      //--- Set the default drawing color to black
      g2d.setPaint(Color.black);

      //--- Print the title

      //Font font = new Font("helvetica", Font.PLAIN, 12);

      g2d.setFont(font);


      //java.awt.geom.AffineTransform at = new java.awt.geom.AffineTransform(pageFormat.getMatrix());

      //java.awt.geom.AffineTransform at = new java.awt.geom.AffineTransform();
        //  at.setToRotation(Math.PI/2.0);

      //g2d.setTransform(at);  this class did not work until I commented this out: Sun bug id:4522900

      //g2d.translate(pageFormat.getImageableX(), pageFormat.getImageableY());

      g2d.setClip(null);


          //FontMetrics fontMetrics = g2d.getFontMetrics();

			FontRenderContext fr = g2d.getFontRenderContext();
		LineMetrics lm = font.getLineMetrics( "HijK", fr );


          double X = pageFormat.getImageableX();

          //double fh = fontMetrics.getHeight();

			double fh = (int)lm.getHeight();

          double Y = pageFormat.getImageableY();


      for (int i = 0; i < lines.size(); i++){



		/* real new lines inside text strings are ignored */

          g2d.drawString ((String)lines.get(i), (float) X , (float) Y);

          Y += fh;   // keeping track of how much was written

      }

      return (PAGE_EXISTS);
    }


  }