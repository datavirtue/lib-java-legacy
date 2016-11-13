/*
 * EnglishDecimalFormat.java
 *
 * Created on July 13, 2007, 4:40 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package datavirtue;

public class EnglishDecimalFormat {
  private static final String[] majorNames = {
    "",
    " Thousand",
    " Million",
    " Billion",
    " Trillion",
    " Quadrillion",
    " Quintillion"
    };

  private static final String[] tensNames = {
    "",
    " Ten",
    " Twenty",
    " Thirty",
    " Forty",
    " Fifty",
    " Sixty",
    " Seventy",
    " Eighty",
    " Ninety"
    };

  private static final String[] numNames = {
    "",
    " One",
    " Two",
    " Three",
    " Four",
    " Five",
    " Six",
    " Seven",
    " Eight",
    " Nine",
    " Ten",
    " Eleven",
    " Twelve",
    " Thirteen",
    " Fourteen",
    " Fifteen",
    " Sixteen",
    " Seventeen",
    " Eighteen",
    " Nineteen"
    };

 private String convertLessThanOneThousand(int number) {
    String soFar;

    if (number % 100 < 20){
        soFar = numNames[number % 100];
        number /= 100;
       }
    else {
        soFar = numNames[number % 10];
        number /= 10;

        soFar = tensNames[number % 10] + soFar;
        number /= 10;
       }
    if (number == 0) return soFar;
    return numNames[number] + " hundred" + soFar;
}

 public String convertDollars (float number){
     
    String phrase = Float.toString(number) ;
    Float num = new Float( phrase ) ;
    int dollars = (int)Math.floor( num ) ;
    int cent = (int)Math.floor( ( num - dollars ) * 100.0f ) ;

    return convert( dollars ) + " Dollars & " + convert( cent ) + " Cents" ;

 }
 
 
public String convert(int number) {
    /* special case */
    if (number == 0) { return "Zero"; }

    String prefix = "";

    if (number < 0) {
        number = -number;
        prefix = "Negative";
      }

    String soFar = "";
    int place = 0;

    do {
      int n = number % 1000;
      if (n != 0){
         String s = convertLessThanOneThousand(n);
         soFar = s + majorNames[place] + soFar;
        }
      place++;
      number /= 1000;
      } while (number > 0);

    return (prefix + soFar).trim();
}


}