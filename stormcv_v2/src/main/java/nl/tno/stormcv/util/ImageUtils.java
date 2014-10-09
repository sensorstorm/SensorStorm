package nl.tno.stormcv.util;

import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.awt.image.ColorConvertOp;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

public class ImageUtils {

	/**
	 * Converts an image to byte buffer representing PNG (bytes as they would exist on disk)
	 * @param image
	 * @param encoding the encoding to be used, one of: png, jpeg, bmp, wbmp, gif
	 * @return byte[] representing the image
	 * @throws IOException if the bytes[] could not be written
	 */
	public static byte[] imageToBytes(BufferedImage image, String encoding) throws IOException{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ImageIO.write(image, encoding, baos);
		return baos.toByteArray();
	}
	
	/**
	 * Converts the provided byte buffer into an BufferedImage
	 * @param buf byte[] of an image as it would exist on disk
	 * @return BufferedImage decoded from buf
	 * @throws IOException
	 */
	public static BufferedImage bytesToImage(byte[] buf) throws IOException{
		ByteArrayInputStream bais = new ByteArrayInputStream(buf);
		return ImageIO.read(bais);
	}
	
	/**
	 * Converts a given image into grayscale
	 * @param src
	 * @return a grayscale image 
	 */
	public static BufferedImage convertToGray(BufferedImage src){
        ColorConvertOp grayOp = new ColorConvertOp(ColorSpace.getInstance(ColorSpace.CS_GRAY), null);
        return grayOp.filter(src,null);
    }
}
