package datavirtue;

import javax.swing.text.*;
import javax.swing.*;
import javax.swing.plaf.basic.BasicComboBoxEditor;
import javax.swing.event.DocumentListener;
import javax.swing.event.UndoableEditListener;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Collection;
/*
 *USE:
 *
 * JComboBox box = AutoCompleteDocument.createAutoCompleteComboBox( listOfWords );
 *
 *
 *
 *
 **/ 



public class AutoCompleteDocument implements Document {
 
    private Collection dictionary;
    private JTextComponent comp;
    private Document delegate;
 
    public AutoCompleteDocument(JTextComponent comp, Collection dictionary) {
        this( comp, dictionary, new PlainDocument() );
    }
 
    public AutoCompleteDocument( JTextComponent field, Collection aDictionary, Document aDelegate ) {
        comp = field;
        dictionary = aDictionary;
        delegate = aDelegate;
    }
 
    public void addDictionaryEntry( String item ) {
        dictionary.add( item );
    }
 
    public void insertString(int offs, String str, AttributeSet a) throws BadLocationException {
        delegate.insertString( offs, str, a );
        
        if (!this.getText(0, this.getLength()).equals("")){  //don't auto complete if string is empty ""
            
        String word = this.autoComplete( this.getText( 0, this.getLength() ) );
        
        if( word != null ) {
        
            delegate.insertString( offs + str.length(), word, a );
            comp.moveCaretPosition( offs + str.length() );
            
        }
        
        }
        
    }
 
    public String autoComplete( String text ) {
        
        //TODO
        
        for( Iterator i = dictionary.iterator(); i.hasNext(); ) {
            
            String word = (String) i.next();
            
            if( word.startsWith( text ) ) {
                return word.substring( text.length() );
            }
        }
        
        return null;
    }
 
    public int getLength() {
        return delegate.getLength();
    }
 
    public void addDocumentListener(DocumentListener listener) {
        delegate.addDocumentListener( listener );
    }
 
    public void removeDocumentListener(DocumentListener listener) {
        delegate.removeDocumentListener( listener );
    }
 
    public void addUndoableEditListener(UndoableEditListener listener) {
        delegate.addUndoableEditListener( listener );
    }
 
    public void removeUndoableEditListener(UndoableEditListener listener) {
        delegate.removeUndoableEditListener( listener );
    }
 
    public Object getProperty(Object key) {
        return delegate.getProperty( key );
    }
 
    public void putProperty(Object key, Object value) {
        delegate.putProperty( key, value );
    }
 
    public void remove(int offs, int len) throws BadLocationException {
        delegate.remove( offs, len );
    }
 
    public String getText(int offset, int length) throws BadLocationException {
        return delegate.getText( offset, length );
    }
 
    public void getText(int offset, int length, Segment txt) throws BadLocationException {
        delegate.getText( offset, length, txt );
    }
 
    public Position getStartPosition() {
        return delegate.getStartPosition();
    }
 
    public Position getEndPosition() {
        return delegate.getEndPosition();
    }
 
    public Position createPosition(int offs) throws BadLocationException {
        return delegate.createPosition( offs );
    }
 
    public Element[] getRootElements() {
        return delegate.getRootElements();
    }
 
    public Element getDefaultRootElement() {
        return delegate.getDefaultRootElement();
    }
 
    public void render(Runnable r) {
        delegate.render( r );
    }
 
    /**
     * Creates a auto completing JTextField.
     *
     * @param dictionary an array of words to use when trying auto completion.
     * @return a JTextField that is initialized as using an auto completing textfield.
     */
    public static JTextField createAutoCompleteTextField( Collection dictionary ) {
        JTextField field = new JTextField();
        AutoCompleteDocument doc = new AutoCompleteDocument( field, dictionary );
        field.setDocument(doc);
        return field;
    }
 
    public static JComboBox createAutoCompleteComboBox( Collection dictionary ) {
        JComboBox box = new JComboBox();
        box.setEditable( true );
        for( Iterator i = dictionary.iterator(); i.hasNext(); ) {
            box.addItem( i.next() );
        }
        box.setEditor( new AutoCompleteComboBoxEditor( dictionary ) );
        return box;
    }
 
    public static void installAutoComplete (JComboBox box, Collection dictionary){
        
        box.setEditable( true );
        for( Iterator i = dictionary.iterator(); i.hasNext(); ) {
            box.addItem( i.next() );
        }
        box.setEditor( new AutoCompleteComboBoxEditor( dictionary ) );
                      
    }
    
    
    public static class AutoCompleteComboBoxEditor extends BasicComboBoxEditor {
        AutoCompleteDocument autoDoc;
 
        public AutoCompleteComboBoxEditor( Collection dictionary ) {
            autoDoc = new AutoCompleteDocument( editor, dictionary );
            editor.setDocument( autoDoc );
        }
    }
 
    public static void main(String args[]) {
        javax.swing.JFrame frame = new javax.swing.JFrame("foo");
        frame.setDefaultCloseOperation( javax.swing.JFrame.EXIT_ON_CLOSE );
        String[] dict = {  "auto",
                           "automobile",
                           "autocrat",
                           "apple",
                           "atom",
                           "concentration",
                           "unification",
                           "graduation" };
        JTextField field = AutoCompleteDocument.createAutoCompleteTextField( Arrays.asList(dict) );
        JComboBox box = AutoCompleteDocument.createAutoCompleteComboBox( Arrays.asList(dict) );
        field.setColumns( 30 );
        frame.getContentPane().setLayout( new BoxLayout( frame.getContentPane(), BoxLayout.X_AXIS ) );
        frame.getContentPane().add( new javax.swing.JLabel("Text Field: ") );
        frame.getContentPane().add(field);
        frame.getContentPane().add( new JLabel("Combo Box: ") );
        frame.getContentPane().add( box );
        frame.pack();
        frame.show();
    }
}

