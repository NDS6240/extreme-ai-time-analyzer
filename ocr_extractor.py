from pdf2image import convert_from_path
import pytesseract
import numpy as np
try:
    import cv2
except ImportError:
    print("WARNING: OpenCV not installed. OCR quality will be lower.")
    print("Please run: pip install opencv-python-headless")
    cv2 = None

def extract_text_with_ocr(file_path: str, lang: str = 'heb+eng') -> str:
    """
    Convert each PDF page to an image and run OCR (Hebrew + English).
    Includes image preprocessing (Grayscale + Thresholding) for better accuracy.
    """
    try:
        pages = convert_from_path(file_path, dpi=300) # הגדלת הרזולוציה ל-300 DPI
    except Exception as e:
        print(f"Debug: Failed to convert PDF to images: {e}")
        return ""

    chunks = []
    for i, page in enumerate(pages):
        try:
            # --- חדש: עיבוד תמונה מקדים עם OpenCV ---
            img_processed = page # ברירת מחדל לתמונה המקורית
            
            if cv2:
                # 1. המרה מ-PIL Image (של pdf2image) ל-OpenCV Image
                img_cv = np.array(page)
                
                # 2. המרה לגווני אפור
                img_gray = cv2.cvtColor(img_cv, cv2.COLOR_BGR2GRAY)
                
                # 3. החלת סף (Thresholding) להפיכת התמונה לשחור/לבן נקי
                # THRESH_OTSU מוצא את הסף האופטימלי אוטומטית
                _, img_processed = cv2.threshold(img_gray, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)
            # --- סוף קטע חדש ---
            
            
            # --- שונה: שינוי הגדרות PSM ל-3 (אוטומטי) ---
            # psm 3: זיהוי פריסה אוטומטי מלא (טוב לטבלאות ועמודות)
            # psm 6: הנחת בלוק טקסט אחיד (מה שהיה קודם)
            config = '--psm 3' 
            
            # הרצת OCR על התמונה *המעובדת*
            txt = pytesseract.image_to_string(img_processed, lang=lang, config=config)
            
            if txt:
                chunks.append(txt)
            else:
                print(f"Debug: No text extracted from page {i+1}")
        except Exception as e:
            print(f"Debug: OCR failed on page {i+1}: {e}")

    return "\n".join(chunks).strip()
    