import json
import os
import boto3
import re
import time
from datetime import datetime
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError
import logging

# Logging setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

region_id = os.environ['region_id']

# DynamoDB config
DYNAMODB_TABLE = os.environ.get("DYNAMODB_SESSION_TABLE", "chat_sessions")
N_LAST_TURNS = int(os.environ.get("N_LAST_TURNS", 6))

dynamodb = boto3.resource('dynamodb')
chat_table = dynamodb.Table(DYNAMODB_TABLE)
sessions_table = dynamodb.Table('session_info')

client = boto3.client(service_name='secretsmanager', region_name=region_id)
bedrock_config = Config(connect_timeout=120, read_timeout=120, retries={'max_attempts': 0})
bedrock_client = boto3.client('bedrock-runtime', region_name=region_id)
bedrock_agent_client = boto3.client("bedrock-agent-runtime", region_name=region_id, config=bedrock_config)

# S3 client for document upload/retrieval
s3_client = boto3.client('s3', region_name=region_id)
S3_BUCKET = os.environ.get("S3_BUCKET", "tenders-rfi-test")

def lambda_handler(event, context):
    headers = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "*",
        "Access-Control-Allow-Headers": "*",
        "Access-Control-Allow-Credentials": "true"
    }
    logger.info(f"Received event: {event}")

    def get_secret():
        try:
            secretId = os.environ['secretId']
            get_secret_value_response = client.get_secret_value(SecretId=secretId)
        except ClientError as e:
            raise e
        else:
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
                return json.loads(secret)
            else:
                return get_secret_value_response['SecretBinary']

    # Credentials & config
    secrets = get_secret()
    kbId = secrets['kbId']

    modelId = os.environ['modelId']
    accept = os.environ['accept']
    contentType = os.environ['contentType']
    numberOfResults = int(os.environ['numberOfResults'])
    anthropic_version = os.environ['anthropic_version']
    max_tokens = int(os.environ['max_tokens'])
    temperature = float(os.environ['temperature'])
    top_p = float(os.environ['top_p'])
    top_k = int(os.environ['top_k'])

    # Mode-specific prompts
    clause_drafting_prompt = '''
אתה עורך דין מומחה בניסוח סעיפים למכרזים ממשלתיים.

תקבל:
- בקשה מהמשתמש לניסוח סעיף מסוים
- פסקאות רלוונטיות ממסמכי מכרזים קודמים ומדריכי רכש ממשלתי
- היסטוריית השיחה אם קיימת

תפקידך:
1. נסח סעיף מפורט ומקצועי המותאם לדרישות המשתמש
2. בסס את הניסוח על הפסקאות הרלוונטיות ממאגר הידע
3. ציין בצורה מפורשת את מקור ההשראה לכל רכיב מרכזי בסעיף (הפנה למסמך ומספר סעיף)
4. הוסף הערות והמלצות רלוונטיות לפי הצורך
5. ודא שהניסוח עומד בדרישות החוק והתקנות הממשלתיות
6. ענה תמיד בעברית
7. היה תמציתי וממוקד

פורמט התשובה:
[ניסוח הסעיף המלא]

הסברים והערות:
[הסברים קצרים על בחירות מרכזיות]

מקורות:
[רשימה של כל המסמכים והסעיפים שעליהם התבסס הניסוח]
'''

    compliance_check_prompt = '''
אתה מומחה לבדיקת התאמה בין מסמכי מכרז להצעות ספקים.

תקבל:
- דרישות מהמכרז (מתוך מסמך דרישות)
- תוכן מהצעת הספק
- פסקאות רלוונטיות ממאגר הידע לצורך השוואה

תפקידך:
1. השווה באופן שיטתי בין דרישות המכרז להצעת הספק
2. זהה התאמות, פערים, אי-התאמות וסעיפים חסרים
3. דרג את חומרת כל אי-התאמה (קריטי, בינוני, מינורי)
4. הפנה למסמכים ממאגר הידע שמדגימים דרישות דומות
5. הפק דוח מסודר וברור
6. ענה תמיד בעברית

פורמט התשובה:
סיכום מנהלים:
[תקציר קצר של רמת ההתאמה הכוללת]

פירוט ממצאים:
[טבלה עם דרישה, מה נמצא בהצעה, רמת התאמה, וחומרה]

המלצות:
[המלצות לטיפול בפערים]

מקורות:
[רשימת מסמכים וסעיפים רלוונטיים ממאגר הידע]
'''

    document_analysis_prompt = '''
אתה עורך דין מנוסה המתמחה בניתוח מסמכים משפטיים ומכרזיים.

תקבל:
- מסמך משפטי/חוזי שהועלה על ידי המשתמש
- שאילתת המשתמש לגבי המסמך
- פסקאות רלוונטיות ממאגר הידע לצורך השוואה

תפקידך:
1. ספק תקציר מנהלים של המסמך (עד 5 משפטים)
2. זהה נקודות סיכון משפטיות, כלכליות או תפעוליות
3. השווה לסעיפים מקבילים ממכרזים אחרים במאגר הידע
4. הצע שיפורים או תיקונים במידת הצורך
5. בסס כל קביעה על מקורות ממאגר הידע
6. ענה תמיד בעברית
7. היה ממוקד ומעשי

פורמט התשובה:
תקציר מנהלים:
[5 משפטים מקסימום]

נקודות סיכון:
[רשימת סיכונים עם דירוג חומרה]

השוואה למכרזים אחרים:
[ממצאים מסעיפים דומים במאגר]

המלצות:
[המלצות מעשיות]

מקורות:
[רשימת מסמכים וסעיפים שעליהם התבססה הבדיקה]
'''

    general_prompt = '''
אתה עוזר דיגיטלי למיטוב תהליכי רכש ממשלתי.

תקבל:
- שאלה כללית מהמשתמש
- פסקאות רלוונטיות ממסמכי מכרז ומדריכי רכש
- היסטוריית שיחה אם קיימת

תפקידך:
1. ענה על השאלה בצורה ברורה ומקצועית
2. בסס את התשובה על המידע ממאגר הידע
3. ציין מקורות למידע (שם מסמך ומספר סעיף)
4. ענה תמיד בעברית
5. היה תמציתי וממוקד

מקורות:
[רשימת כל המסמכים והסעיפים הרלוונטיים]
'''

    classification_prompt = '''
אתה מסווג שאילתות משתמש לקטגוריות.

קטגוריות אפשריות:
1. clause_drafting - בקשה לניסוח סעיף חדש, כתיבת סעיף, יצירת נוסח
   דוגמאות: "נסח סעיף...", "כתוב סעיף...", "צור נוסח ל...", "איך לנסח..."

2. compliance_check - בדיקת התאמה, השוואה בין מסמכים, בדיקת עמידה בדרישות
   דוגמאות: "בדוק התאמה...", "השווה בין...", "האם עומד בדרישות...", "מה הפערים..."

3. document_analysis - ניתוח מסמך, תקציר, זיהוי סיכונים, סקירת חוזה
   דוגמאות: "נתח את המסמך...", "תן תקציר...", "מה הסיכונים...", "סקור את החוזה..."

4. general - שאלות כלליות, הבהרות, מידע על תהליכים
   דוגמאות: "מה זה...", "הסבר לי...", "איך עובד...", "מה ההבדל..."

השאילתה הנוכחית:
{query}

אם יש שאילתות קודמות, קח אותן בחשבון להבנת ההקשר.

ענה רק עם אחת מהמילים: clause_drafting, compliance_check, document_analysis, general
אל תוסיף שום הסבר או טקסט נוסף.
'''

    def retrieve(query, kbId, numberOfResults, data_source_id=None):
        retrieval_config = {
            'vectorSearchConfiguration': {
                'numberOfResults': numberOfResults,
                'overrideSearchType': "HYBRID"
            }
        }
        
        if data_source_id:
            retrieval_config['vectorSearchConfiguration']['filter'] = {
                'equals': {
                    'key': 'x-amz-bedrock-kb-data-source-id',
                    'value': data_source_id
                }
            }
        
        return bedrock_agent_client.retrieve(
            retrievalQuery={'text': query},
            knowledgeBaseId=kbId,
            retrievalConfiguration=retrieval_config
        )

    def get_contexts(retrievalResults):
        contexts = []
        sources = []
        for result in retrievalResults:
            contexts.append(result['content']['text'])
            source_uri = result['location']['s3Location']['uri'].rsplit('/', 1)[-1]
            sources.append(source_uri)
        return contexts, sources

    def clean_text(text):
        text = re.sub(r"\n*</?[^>]+>\n*", "", text).strip()
        return text.replace(".pdf", "").replace(".docx", "")

    def get_document_from_s3(document_key):
        """Retrieve document content from S3 and extract text"""
        try:
            # Determine file type from extension
            file_extension = document_key.lower().split('.')[-1]

            # For DOCX files, first try to get pre-processed .txt version
            if file_extension in ['docx', 'doc']:
                txt_key = document_key.rsplit('.', 1)[0] + '.txt'
                try:
                    logger.info(f"Checking for pre-processed text file: {txt_key}")
                    txt_response = s3_client.get_object(Bucket=S3_BUCKET, Key=txt_key)
                    txt_content = txt_response['Body'].read()

                    # Try to decode with various encodings
                    for encoding in ['utf-8', 'windows-1255', 'iso-8859-8', 'latin-1', 'cp862']:
                        try:
                            decoded_text = txt_content.decode(encoding)
                            logger.info(f"Successfully loaded pre-processed text file: {txt_key}")
                            return decoded_text
                        except UnicodeDecodeError:
                            continue
                    logger.warning(f"Could not decode pre-processed text file: {txt_key}")
                except ClientError as e:
                    if e.response['Error']['Code'] == 'NoSuchKey':
                        logger.info(f"No pre-processed text file found for {document_key}, will extract from DOCX")
                    else:
                        logger.warning(f"Error accessing pre-processed text file: {e}")

            # If we get here, either it's not a DOCX or the .txt file wasn't found
            # Proceed with normal extraction
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=document_key)
            file_content = response['Body'].read()

            if file_extension == 'pdf':
                # Extract text from PDF using PyPDF2
                try:
                    import PyPDF2
                    import io
                    pdf_file = io.BytesIO(file_content)
                    pdf_reader = PyPDF2.PdfReader(pdf_file)
                    text = ""
                    for page in pdf_reader.pages:
                        text += page.extract_text() + "\n"
                    return text
                except Exception as pdf_error:
                    logger.error(f"Error extracting PDF text: {pdf_error}")
                    return None
                    
            elif file_extension in ['docx', 'doc']:
                # FALLBACK: Extract text from DOCX using manual XML parsing
                # NOTE: This is a fallback method. For best results, upload a pre-processed
                # .txt file alongside the .docx file (e.g., "document.txt" for "document.docx")
                try:
                    import zipfile
                    import io
                    import xml.etree.ElementTree as ET
                    
                    # DOCX files are ZIP archives containing XML
                    docx_zip = zipfile.ZipFile(io.BytesIO(file_content))
                    
                    # Read the main document XML
                    xml_content = docx_zip.read('word/document.xml')
                    tree = ET.fromstring(xml_content)
                    
                    # Namespace for Word documents
                    namespace = {'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}
                    
                    # Extract all text from paragraphs
                    paragraphs = []
                    for paragraph in tree.findall('.//w:p', namespace):
                        texts = []
                        for text_elem in paragraph.findall('.//w:t', namespace):
                            if text_elem.text:
                                texts.append(text_elem.text)
                        if texts:
                            paragraphs.append(''.join(texts))
                    
                    return '\n'.join(paragraphs)
                    
                except Exception as docx_error:
                    logger.error(f"Error extracting DOCX text: {docx_error}")
                    return None
                
            elif file_extension == 'txt':
                # Try multiple encodings for text files (including Hebrew encodings)
                for encoding in ['utf-8', 'windows-1255', 'iso-8859-8', 'latin-1', 'cp862']:
                    try:
                        return file_content.decode(encoding)
                    except UnicodeDecodeError:
                        continue
                logger.error(f"Could not decode text file with any encoding: {document_key}")
                return None
                
            else:
                logger.error(f"Unsupported file type: {file_extension} for file: {document_key}")
                return None
                
        except Exception as e:
            logger.error(f"Error retrieving document from S3: {e}")
            return None

    def classify_query(query, prev_turns=None):
        """Classify user query to determine the appropriate mode"""
        try:
            # Build context from previous user queries (last 4)
            context_queries = ""
            if prev_turns:
                recent_user_queries = []
                for turn in prev_turns[-4:]:  # Last 4 turns
                    user_msg = turn.get("user_message", "").strip()
                    if user_msg:
                        recent_user_queries.append(user_msg)

                if recent_user_queries:
                    context_queries = "\n\nשאילתות קודמות בשיחה:\n" + "\n".join([f"{i+1}. {q}" for i, q in enumerate(recent_user_queries)])

            classification_msg = classification_prompt.format(query=query) + context_queries

            messages = [{"role": 'user', "content": classification_msg}]
            invoke_json = json.dumps({
                "anthropic_version": anthropic_version,
                "max_tokens": 50,  # We only need one word response
                "messages": messages,
                "temperature": 0.0  # Low temperature for consistent classification
            })

            response = bedrock_client.invoke_model(
                body=invoke_json,
                modelId=modelId,
                accept=accept,
                contentType=contentType
            )

            response_body = json.loads(response.get('body').read())
            detected_mode = response_body.get('content')[0]['text'].strip().lower()

            # Validate mode
            valid_modes = ['clause_drafting', 'compliance_check', 'document_analysis', 'general']
            if detected_mode not in valid_modes:
                logger.warning(f"Invalid mode detected: {detected_mode}, defaulting to general")
                return 'general'

            logger.info(f"Classified query as mode: {detected_mode}")
            return detected_mode

        except Exception as e:
            logger.error(f"Error classifying query: {e}")
            return 'general'  # Default to general mode on error

    def create_prompt(mode, instructions, contexts, sources, query, prev_turns=None, 
                     tender_doc=None, vendor_proposal=None, uploaded_doc=None, max_history=3):
        # Build conversation history
        conversation = ""
        if prev_turns:
            for turn in prev_turns[-max_history:]:
                user_msg = turn.get("user_message", "").strip()
                agent_msg = turn.get("model_response", "").strip()
                if user_msg:
                    conversation += f"<user>\n{user_msg}\n</user>\n"
                if agent_msg:
                    conversation += f"<agent>\n{agent_msg}\n</agent>\n"

        # Construct prompt
        prompt = f"""
אתה עוזר AI למיטוב תהליכי רכש ממשלתי. פעל לפי ההוראות בקפידה.

<instructions>
{instructions}
</instructions>

<context_from_knowledge_base>
<paragraphs>
{contexts}
</paragraphs>
<sources>
{sources}
</sources>
</context_from_knowledge_base>
"""

        # Add mode-specific context
        if mode == "compliance_check" and tender_doc and vendor_proposal:
            prompt += f"""
<tender_requirements>
{tender_doc}
</tender_requirements>

<vendor_proposal>
{vendor_proposal}
</vendor_proposal>
"""

        if mode == "document_analysis" and uploaded_doc:
            prompt += f"""
<uploaded_document>
{uploaded_doc}
</uploaded_document>
"""

        if conversation:
            prompt += f"""
<conversation_history>
{conversation}
</conversation_history>
"""

        prompt += f"""
<question>
{query}
</question>

ענה כעוזר מקצועי ומועיל:
"""
        return prompt

    try:
        body = json.loads(event.get('body', '{}'))
        query = body.get('query')
        session_id = body.get('session_id')
        user_id = body.get('user_id')
        
        # Mode can be explicitly provided or auto-detected
        mode = body.get('mode')
        
        # Mode-specific parameters
        tender_doc_key = 'joint-tender.pdf' #body.get('tender_document')
        vendor_proposal_key = 'proposal-surpass.docx' #body.get('vendor_proposal')
        uploaded_doc_key = body.get('uploaded_document')

        if not query:
            return {
                'statusCode': 400,
                'headers': headers,
                'body': json.dumps({'error_message': 'query is required'})
            }

        # Load previous conversation turns first (needed for classification)
        prev_turns = None
        if session_id:
            resp = chat_table.query(
                KeyConditionExpression=Key('session_id').eq(session_id),
                ScanIndexForward=False,
                Limit=N_LAST_TURNS
            )
            prev_turns = list(reversed(resp.get('Items', [])))

        # Auto-detect mode if not provided
        if not mode:
            logger.info("Mode not provided, classifying query...")
            mode = classify_query(query, prev_turns)
        else:
            logger.info(f"Mode explicitly provided: {mode}")

        # Validate mode-specific requirements
        if mode == "compliance_check" and (not tender_doc_key or not vendor_proposal_key):
            # If classified as compliance_check but documents not provided, fallback to general
            logger.warning("Classified as compliance_check but documents not provided, switching to general mode")
            mode = "general"

        if mode == "document_analysis" and not uploaded_doc_key:
            # If classified as document_analysis but no document provided, fallback to general
            logger.warning("Classified as document_analysis but no document provided, switching to general mode")
            mode = "general"

        # Check if this is the first turn
        is_first_turn = (prev_turns is None) or (len(prev_turns) == 0)

        # Select prompt based on mode
        mode_prompts = {
            'clause_drafting': clause_drafting_prompt,
            'compliance_check': compliance_check_prompt,
            'document_analysis': document_analysis_prompt,
            'general': general_prompt
        }
        instructions = mode_prompts.get(mode, general_prompt)

        # Add topic generation instruction for first turn
        if is_first_turn:
            instructions += (
                "\n\nבתחילת התשובה, כתוב כותרת נושא קצרה (עד 10 מילים) "
                "בעברית המסכמת את נושא השיחה. "
                "לאחר מכן, בשורה חדשה, המשך עם התשובה המלאה והמפורטת."
            )

        # Retrieve relevant contexts from knowledge base
        response_kb = retrieve(query, kbId, numberOfResults)
        contexts, sources = get_contexts(response_kb['retrievalResults'])

        # Load documents from S3 if needed
        tender_doc = None
        vendor_proposal = None
        uploaded_doc = None

        if mode == "compliance_check":
            tender_doc = get_document_from_s3(tender_doc_key)
            vendor_proposal = get_document_from_s3(vendor_proposal_key)
            if not tender_doc or not vendor_proposal:
                return {
                    'statusCode': 400,
                    'headers': headers,
                    'body': json.dumps({'error_message': 'Failed to retrieve documents from S3'})
                }

        if mode == "document_analysis" and uploaded_doc_key:
            uploaded_doc = get_document_from_s3(uploaded_doc_key)
            if not uploaded_doc:
                return {
                    'statusCode': 400,
                    'headers': headers,
                    'body': json.dumps({'error_message': 'Failed to retrieve uploaded document from S3'})
                }

        # Create prompt
        prompt = create_prompt(
            mode, 
            instructions, 
            "\n\n".join(contexts), 
            "\n".join(sources), 
            query, 
            prev_turns,
            tender_doc=tender_doc,
            vendor_proposal=vendor_proposal,
            uploaded_doc=uploaded_doc
        )

        # Adjust max_tokens for complex modes
        adjusted_max_tokens = max_tokens
        if mode in ['compliance_check', 'document_analysis']:
            adjusted_max_tokens = max_tokens * 2

        messages = [{"role": 'user', "content": prompt}]
        invoke_json = json.dumps({
            "anthropic_version": anthropic_version,
            "max_tokens": adjusted_max_tokens,
            "messages": messages,
            "temperature": temperature,
            "top_p": top_p,
            "top_k": top_k
        })

        start_time = time.time()
        response = bedrock_client.invoke_model(
            body=invoke_json,
            modelId=modelId,
            accept=accept,
            contentType=contentType
        )
        response_time = time.time() - start_time
        response_time_ms = round(response_time * 1000)

        response_body = json.loads(response.get('body').read())
        response_text = clean_text(response_body.get('content')[0]['text'])

        # Extract topic for first turn
        topic_line = None
        if is_first_turn:
            lines = response_text.strip().split("\n", 1)
            topic_line = lines[0].strip()

            # Ensure topic line is max 10 words
            topic_words = topic_line.split()
            if len(topic_words) > 10:
                topic_line = " ".join(topic_words[:10])

            # If model didn't output a topic, generate a fallback
            if not topic_line or len(topic_line.split()) < 2:
                topic_line = "נושא: מיטוב תהליכי רכש"

            # Rebuild final response text
            if len(lines) > 1:
                response_text = topic_line + "\n" + lines[1].strip()
            else:
                response_text = topic_line

        # Save turn in DynamoDB
        if session_id:
            now = datetime.utcnow()
            timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            turn_number = 1
            if prev_turns:
                turn_number = prev_turns[-1].get('turn_number', 0) + 1

            try:
                chat_table.put_item(Item={
                    "session_id": session_id,
                    "timestamp": timestamp,
                    "turn_number": turn_number,
                    "user_message": query,
                    "model_response": response_text,
                    "active": True,
                    "last_active": int(time.time()),
                    "user_id": user_id,
                    "mode": mode
                })
                
                if is_first_turn:
                    sessions_table.put_item(Item={
                        "session_id": session_id,
                        "last_active": int(time.time()),
                        "user_id": user_id
                    })
                else:
                    sessions_table.update_item(
                        Key={"user_id": user_id, "session_id": session_id},
                        UpdateExpression="SET last_active = :last_active",
                        ExpressionAttributeValues={":last_active": int(time.time())}
                    )
            except Exception as db_err:
                logger.error(f"Failed to write to DynamoDB: {db_err}")

        response_body = {
            "answer": response_text,
            "response_time_ms": response_time_ms,
            "session_id": session_id or None,
            "mode": mode
        }
        
        if is_first_turn:
            response_body["topic"] = topic_line or None

        logger.info(f"Response text: {response_text}")
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps(response_body, ensure_ascii=False)
        }

    except Exception as e:
        logger.error(f"Internal Server Error: {e}")
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps({'error_message': 'Internal Server Error'})
        }

