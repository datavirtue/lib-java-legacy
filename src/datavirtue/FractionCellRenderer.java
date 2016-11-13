package datavirtue;

import java.text.*;
import javax.swing.*;
import javax.swing.table.*;

// A holder for data and an associated icon
public class FractionCellRenderer extends DefaultTableCellRenderer {
	public FractionCellRenderer(int integer, int fraction, int align) {
		this.integer = integer;		// maximum integer digits
		this.fraction = fraction;	// exact number of fraction digits
		this.align = align;			// alignment (LEFT, CENTER, RIGHT)
	}

	protected void setValue(Object value) {
		if (value != null && value instanceof Number) {
			formatter.setMaximumIntegerDigits(integer);
			formatter.setMaximumFractionDigits(fraction);
			formatter.setMinimumFractionDigits(fraction);
			setText(formatter.format(((Number)value).doubleValue()));							
		} else {
			super.setValue(value);
		}
		setHorizontalAlignment(align);
	}

	protected int integer;
	protected int fraction;
	protected int align;
	protected static NumberFormat formatter = NumberFormat.getInstance();
}