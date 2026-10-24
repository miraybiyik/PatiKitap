"""PatiKitap business API. All mutations use a verified Supabase identity."""
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import uuid4, UUID
from io import BytesIO
import csv
import re
from starlette.concurrency import run_in_threadpool
import httpx
from fastapi import FastAPI, Depends, HTTPException, Query, Request as HttpRequest, Response
from fastapi.responses import JSONResponse
from operations import OperationalMiddleware
import logging
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from sqlalchemy import select, text, func, or_, inspect
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from PIL import Image, UnidentifiedImageError
from database import engine
from models import (Base, Profile, Book, Request, Transaction, Delivery, SupportRecord,
                    Favorite, Notification, Complaint, AuditLog, now)
from schemas import (BookInput, ProfileInput, RequestInput, RequestAction, DeliveryInput,
                     ComplaintInput, ResolutionInput, SupportInput, ModerateInput, UserStateInput)
from auth import Identity, identity, SUPABASE_URL, SUPABASE_KEY

@asynccontextmanager
async def lifespan(app):
    # PostgreSQL schema is versioned through Supabase migrations, never changed on startup.
    if engine.dialect.name == 'sqlite':
        existing = inspect(engine)
        if existing.has_table('books') and 'owner_id' not in {c['name'] for c in existing.get_columns('books')}:
            raise RuntimeError('Eski SQLite şeması. Supabase bağlantısını kullanın veya yeni bir SQLite dosyası seçin.')
        Base.metadata.create_all(engine)
    yield
app = FastAPI(title='PatiKitap API', version='0.3.0', lifespan=lifespan)
app.add_middleware(OperationalMiddleware,supabase_url=SUPABASE_URL)
@app.middleware("http")
async def disable_csp_for_docs(request, call_next):
    response = await call_next(request)
    if any(request.url.path.startswith(p) for p in ["/docs", "/openapi.json", "/redoc"]):
        # Starlette response headers MutableHeaders nesnesidir, del ile silinir:
        if "content-security-policy" in response.headers:
            del response.headers["content-security-policy"]
    return response
logging.getLogger('patikitap.http').setLevel(logging.INFO)

@app.exception_handler(IntegrityError)
async def conflict_handler(request, exc):
    return JSONResponse(status_code=409, content={'detail':'Bu kayıt zaten var veya işlem başka bir kullanıcı tarafından değiştirildi. Yenileyip tekrar dene.'})

@app.exception_handler(SQLAlchemyError)
async def db_error_handler(request, exc):
    return JSONResponse(status_code=503, content={'detail':'Veritabanı işlemi tamamlanamadı. Daha sonra yeniden dene.'})

def db():
    with Session(engine, expire_on_commit=False) as session:
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
DB = Annotated[Session, Depends(db, scope='function')]

def principal(session: DB, user: Annotated[Identity, Depends(identity)]):
    profile = session.get(Profile, user.id)
    if profile is None:
        profile = Profile(id=user.id, display_name=user.name)
        # A savepoint handles simultaneous first requests without aborting their transactions.
        try:
            with session.begin_nested():
                session.add(profile)
                session.flush()
        except IntegrityError:
            profile=session.get(Profile,user.id)
    if profile is None or profile.disabled: raise HTTPException(403,'Hesap kullanıma kapalı.')
    return user
USER = Annotated[Identity, Depends(principal)]
def admin(user: USER):
    if not user.is_admin: raise HTTPException(403,'Bu işlem yönetici yetkisi gerektirir.')
    return user
ADMIN = Annotated[Identity, Depends(admin)]

def record(row):
    return {column.key:getattr(row,column.key) for column in row.__table__.columns}
def get(session, model, key):
    row=session.get(model,key)
    if row is None: raise HTTPException(404,'Kayıt bulunamadı.')
    return row
def audit(session,user,action,row,detail=''):
    session.add(AuditLog(actor_id=user.id, action=action, entity_type=row.__tablename__, entity_id=str(row.id), detail=detail))
def notify(session, uid, message, transaction_id=None):
    session.add(Notification(user_id=uid,message=message,transaction_id=transaction_id))
def participant(tx,user):
    if user.id not in (tx.owner_id,tx.requester_id): raise HTTPException(403,'Bu işlem sana ait değil.')
def lock_books(session,ids):
    return {b.id:b for b in session.scalars(select(Book).where(Book.id.in_(sorted(set(ids)))).order_by(Book.id).with_for_update()).all()}
def lock_tx(session,tid):
    tx=get(session,Transaction,tid)
    ids=[tx.book_id]+([tx.offered_book_id] if tx.offered_book_id else [])
    books=lock_books(session,ids)
    session.refresh(tx,with_for_update=True)
    return tx,books

def image_check(url,user):
    if url and not re.fullmatch(r'book-media/'+re.escape(user.id)+r'/[0-9a-f]{32}\.jpg',url):
        raise HTTPException(422,'Yalnızca kendi yüklediğin kitap fotoğraflarını kullanabilirsin.')

def cancel_transaction(session,tx,books,user,reason):
    if tx.status in ('completed','cancelled'): raise HTTPException(409,'Bu işlem kapatılamaz.')
    tx.status='cancelled'
    for book in books.values():
        if book.status=='reserved': book.status='active'
    req=get(session,Request,tx.request_id); req.status='cancelled'
    for delivery in session.scalars(select(Delivery).where(Delivery.transaction_id==tx.id)):
        delivery.status='cancelled'
    audit(session,user,'transaction.cancelled',tx,reason)
    for uid in (tx.owner_id,tx.requester_id): notify(session,uid,'İşlem iptal edildi; kitaplar yeniden paylaşıma açıldı.',tx.id)

@app.get('/api/health')
def health(): return {'status':'ok','mode':'prototype','version':'0.3.0'}
@app.get('/api/health/database')
def database_health(session: DB):
    session.execute(text('SELECT 1'))
    return {'status':'ok','database':engine.dialect.name}
@app.get('/api/config')
def config(): return {'supabase_url':SUPABASE_URL,'supabase_publishable_key':SUPABASE_KEY,'mode':'simulation','support_rate_bps':2000}
@app.get('/api/me')
def me(session: DB,user: USER):
    return {**record(get(session,Profile,user.id)),'is_admin':user.is_admin}
@app.patch('/api/me')
def edit_profile(data: ProfileInput,session: DB,user: USER):
    profile=get(session,Profile,user.id)
    for key,value in data.model_dump().items(): setattr(profile,key,value)
    return {**record(profile),'is_admin':user.is_admin}
@app.get('/api/books')
def list_books(session: DB, q: str='', category: str='', kind: str='', offset: int=Query(0,ge=0),limit:int=Query(200,ge=1,le=200)):
    statement=select(Book).where(Book.status=='active')
    if q.strip(): statement=statement.where(or_(Book.title.ilike('%'+q.strip()[:160]+'%'),Book.author.ilike('%'+q.strip()[:160]+'%')))
    if category: statement=statement.where(Book.category==category)
    if kind: statement=statement.where(Book.kind==kind)
    return [record(b) for b in session.scalars(statement.order_by(Book.id.desc()).offset(offset).limit(limit))]
@app.get('/api/me/books')
def own_books(session: DB,user: USER, offset:int=Query(0,ge=0), limit:int=Query(100,ge=1,le=200)):
    return [record(b) for b in session.scalars(select(Book).where(Book.owner_id==user.id).order_by(Book.id.desc()).offset(offset).limit(limit))]
@app.post('/api/books',status_code=201)
def create_book(data: BookInput,session: DB,user: USER):
    image_check(data.image_url,user)
    book=Book(**data.model_dump(),owner_id=user.id,is_demo=False)
    session.add(book);session.flush();audit(session,user,'book.created',book)
    return record(book)
@app.put('/api/books/{bid}')
def update_book(bid:int,data:BookInput,session:DB,user:USER):
    book=lock_books(session,[bid]).get(bid)
    if not book: raise HTTPException(404,'Kitap bulunamadı.')
    if book.owner_id!=user.id: raise HTTPException(403,'Bu ilan sana ait değil.')
    if book.status!='active': raise HTTPException(409,'Yalnızca aktif ilan düzenlenebilir.')
    if session.scalar(select(Request.id).where(or_(Request.book_id==bid,Request.offered_book_id==bid),Request.status=='pending').limit(1)):
        raise HTTPException(409,'Bekleyen talepleri sonuçlandırdıktan sonra ilanı düzenleyebilirsin.')
    image_check(data.image_url,user)
    for key,value in data.model_dump().items(): setattr(book,key,value)
    audit(session,user,'book.updated',book)
    return record(book)
@app.delete('/api/books/{bid}')
def archive_book(bid:int,session:DB,user:USER):
    book=lock_books(session,[bid]).get(bid)
    if not book: raise HTTPException(404,'Kitap bulunamadı.')
    if book.owner_id!=user.id: raise HTTPException(403,'Bu ilan sana ait değil.')
    if book.status not in ('active','archived'): raise HTTPException(409,'Bu ilan şu anda arşivlenemez.')
    book.status='archived'
    for req in session.scalars(select(Request).where(or_(Request.book_id==bid,Request.offered_book_id==bid),Request.status=='pending')):
        req.status='rejected';notify(session,req.requester_id,'İlgili kitap arşivlendiği için talebin kapatıldı.')
    audit(session,user,'book.archived',book)
    return {'status':'archived'}
def normalize_image(content):
    try:
        with Image.open(BytesIO(content)) as im:
            if im.format not in ('JPEG','PNG','WEBP') or im.width*im.height>20000000: raise ValueError()
            im.load();im=im.convert('RGB');im.thumbnail((1600,1600))
            output=BytesIO();im.save(output,'JPEG',quality=85)
    except (UnidentifiedImageError,ValueError,OSError,Image.DecompressionBombError):
        raise HTTPException(422,'Geçerli bir JPEG, PNG veya WebP fotoğraf seç.') from None
    return output.getvalue()

@app.post('/api/uploads/image',status_code=201)
async def upload_image(request:HttpRequest,user:USER):
    # Stream with a hard bound; never buffer an unbounded request body.
    content=bytearray()
    async for chunk in request.stream():
        content.extend(chunk)
        if len(content)>5*1024*1024: raise HTTPException(413,'En fazla 5 MB fotoğraf yükleyebilirsin.')
    image_bytes=await run_in_threadpool(normalize_image,bytes(content))
    path=f'{user.id}/{uuid4().hex}.jpg'
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response=await client.post(f'{SUPABASE_URL}/storage/v1/object/book-media/{path}',headers={'apikey':SUPABASE_KEY,'Authorization':f'Bearer {user.token}','Content-Type':'image/jpeg'},content=image_bytes)
        if response.status_code not in (200,201): raise HTTPException(502,'Fotoğraf kaydedilemedi. Yeniden giriş yapıp dene.')
    except httpx.RequestError: raise HTTPException(503,'Fotoğraf servisine ulaşılamadı.') from None
    return {'url':f'book-media/{path}'}
@app.get('/api/me/favorites')
def favorites(session:DB,user:USER):
    return [row for row in session.scalars(select(Favorite.book_id).where(Favorite.user_id==user.id))]
@app.put('/api/me/favorites/{bid}')
def save_favorite(bid:int,session:DB,user:USER):
    get(session,Book,bid)
    if not session.get(Favorite,(user.id,bid)): session.add(Favorite(user_id=user.id,book_id=bid))
    return {'saved':True}
@app.delete('/api/me/favorites/{bid}')
def unsave_favorite(bid:int,session:DB,user:USER):
    row=session.get(Favorite,(user.id,bid))
    if row: session.delete(row)
    return {'saved':False}
@app.post('/api/books/{bid}/requests',status_code=201)
def create_request(bid:int,data:RequestInput,session:DB,user:USER):
    ids=[bid]+([data.offered_book_id] if data.offered_book_id else [])
    books=lock_books(session,ids);book=books.get(bid)
    if not book: raise HTTPException(404,'Kitap bulunamadı.')
    if book.is_demo or not book.owner_id: raise HTTPException(409,'Örnek ilanlar talep edilemez. Gerçek bir kullanıcı ilanı seç.')
    if book.owner_id==user.id: raise HTTPException(409,'Kendi kitabını talep edemezsin.')
    if get(session,Profile,book.owner_id).disabled: raise HTTPException(409,'İlan sahibi şu anda işlem yapamıyor.')
    if book.status!='active': raise HTTPException(409,'Kitap artık paylaşıma açık değil.')
    if book.kind=='Takas':
        offered=books.get(data.offered_book_id)
        if not offered or offered.id==bid or offered.owner_id!=user.id or offered.kind!='Takas' or offered.status!='active':
            raise HTTPException(422,'Takas için kendi aktif takas ilanlarından birini seç.')
    elif data.offered_book_id: raise HTTPException(422,'Bu ilan takas ilanı değil.')
    if session.scalar(select(Request.id).where(Request.book_id==bid,Request.requester_id==user.id,Request.status=='pending')):
        raise HTTPException(409,'Bu kitap için bekleyen talebin var.')
    row=Request(book_id=bid,requester_id=user.id,**data.model_dump());session.add(row);session.flush()
    notify(session,book.owner_id,f'“{book.title}” için yeni bir talep var.');audit(session,user,'request.created',row)
    return record(row)
@app.get('/api/me/requests')
def requests(session:DB,user:USER, offset:int=Query(0,ge=0), limit:int=Query(100,ge=1,le=200)):
    from sqlalchemy.orm import aliased
    offered=aliased(Book)
    rows=session.execute(select(Request,Book.title,Book.owner_id,offered.title).join(Book,Book.id==Request.book_id).outerjoin(offered,offered.id==Request.offered_book_id).where(or_(Request.requester_id==user.id,Book.owner_id==user.id)).order_by(Request.id.desc()).offset(offset).limit(limit))
    return [{**record(r),'book_title':title,'owner_id':owner,'offered_title':other} for r,title,owner,other in rows]

@app.post('/api/requests/{rid}/action')
def request_action(rid:int,data:RequestAction,session:DB,user:USER):
    row=get(session,Request,rid);ids=[row.book_id]+([row.offered_book_id] if row.offered_book_id else [])
    books=lock_books(session,ids);session.refresh(row,with_for_update=True);book=books[row.book_id]
    if row.status!='pending': raise HTTPException(409,'Talep zaten sonuçlanmış.')
    if data.action=='cancel':
        if row.requester_id!=user.id: raise HTTPException(403,'Yalnızca talep sahibi iptal edebilir.')
        row.status='cancelled';notify(session,book.owner_id,'Bir kitap talebi geri çekildi.')
    else:
        if book.owner_id!=user.id: raise HTTPException(403,'Yalnızca ilan sahibi karar verebilir.')
        if data.action=='reject':
            row.status='rejected';notify(session,row.requester_id,f'“{book.title}” talebin kabul edilmedi.')
        else:
            if book.status!='active': raise HTTPException(409,'Kitap artık uygun değil.')
            if get(session,Profile,row.requester_id).disabled: raise HTTPException(409,'Talep sahibi şu anda işlem yapamıyor.')
            if row.offered_book_id:
                offered=books[row.offered_book_id]
                if offered.status!='active' or offered.owner_id!=row.requester_id or offered.kind!='Takas': raise HTTPException(409,'Teklif edilen kitap artık uygun değil.')
            tx=Transaction(request_id=row.id,book_id=book.id,offered_book_id=row.offered_book_id,owner_id=book.owner_id,
                requester_id=row.requester_id,kind=book.kind,price_cents=book.price*100 if book.kind=='Satış' else 0,
                support_cents=book.price*20 if book.kind=='Satış' else 0,fee_cents=0,support_rate_bps=2000)
            session.add(tx);session.flush();row.status='accepted'
            for b in books.values(): b.status='reserved'
            session.add(Delivery(transaction_id=tx.id,sender_id=book.owner_id,recipient_id=row.requester_id,method=row.delivery_method,shipping_payer=row.shipping_payer))
            if row.offered_book_id: session.add(Delivery(transaction_id=tx.id,sender_id=row.requester_id,recipient_id=book.owner_id,method=row.delivery_method,shipping_payer=row.shipping_payer))
            for other in session.scalars(select(Request).where(Request.id!=row.id,Request.status=='pending',or_(Request.book_id.in_(ids),Request.offered_book_id.in_(ids)))):
                other.status='rejected';notify(session,other.requester_id,'İlgili kitap başka bir işlem için ayrıldı; talebin kapatıldı.')
            notify(session,row.requester_id,f'“{book.title}” talebin kabul edildi.',tx.id)
            audit(session,user,'transaction.created',tx)
    audit(session,user,f'request.{row.status}',row)
    return record(row)
@app.get('/api/me/transactions')
def transactions(session:DB,user:USER, offset:int=Query(0,ge=0), limit:int=Query(100,ge=1,le=200)):
    rows=session.execute(select(Transaction,Book.title).join(Book,Book.id==Transaction.book_id).where(or_(Transaction.owner_id==user.id,Transaction.requester_id==user.id)).order_by(Transaction.id.desc()).offset(offset).limit(limit)).all()
    deliveries={t.id:[] for t,_ in rows}
    if deliveries:
        for d in session.scalars(select(Delivery).where(Delivery.transaction_id.in_(deliveries))):deliveries[d.transaction_id].append(record(d))
    return [{**record(t),'book_title':title,'deliveries':deliveries[t.id]} for t,title in rows]

@app.patch('/api/deliveries/{did}')
def dispatch(did:int,data:DeliveryInput,session:DB,user:USER):
    delivery=get(session,Delivery,did);tx,_=lock_tx(session,delivery.transaction_id);session.refresh(delivery)
    if delivery.sender_id!=user.id: raise HTTPException(403,'Yalnızca gönderen teslimat bilgisi girebilir.')
    if tx.status not in ('accepted','in_delivery') or delivery.status not in ('pending','dispatched'): raise HTTPException(409,'Teslimat bu durumda güncellenemez.')
    if delivery.method=='cargo' and (not data.tracking_number or not data.carrier): raise HTTPException(422,'Kargo firması ve takip numarası gerekli.')
    if delivery.method!='cargo' and not data.meeting_point: raise HTTPException(422,'Buluşma veya teslim noktası gerekli.')
    for k,v in data.model_dump().items(): setattr(delivery,k,v)
    if delivery.method!='cargo': delivery.cost_cents=0
    delivery.status='dispatched';tx.status='in_delivery'
    notify(session,delivery.recipient_id,'Teslimat bilgileri güncellendi. İşlem ekranından takip edebilirsin.',tx.id);audit(session,user,'delivery.dispatched',delivery)
    return record(delivery)
@app.post('/api/transactions/{tid}/confirm')
def confirm(tid:int,session:DB,user:USER):
    tx,books=lock_tx(session,tid);participant(tx,user)
    if tx.status=='completed': return record(tx)
    if tx.status!='in_delivery': raise HTTPException(409,'Önce teslimat bilgileri girilmeli; ihtilaflı işlem onaylanamaz.')
    deliveries=session.scalars(select(Delivery).where(Delivery.transaction_id==tx.id)).all()
    if not deliveries or any(d.status=='pending' for d in deliveries): raise HTTPException(409,'Önce tüm teslimatlar hazırlanmalı.')
    if user.id==tx.owner_id: tx.owner_confirmed=True
    else: tx.requester_confirmed=True
    for d in deliveries:
        if d.recipient_id==user.id: d.status='delivered'
    if tx.owner_confirmed and tx.requester_confirmed:
        tx.status='completed';tx.completed_at=now()
        for b in books.values(): b.status='completed'
        for d in deliveries: d.status='delivered'
        if tx.support_cents>0: session.add(SupportRecord(transaction_id=tx.id,amount_cents=tx.support_cents))
        for uid in (tx.owner_id,tx.requester_id): notify(session,uid,'İki taraf da onayladı; paylaşım tamamlandı.',tx.id)
    audit(session,user,'transaction.confirmed',tx)
    return record(tx)
@app.post('/api/transactions/{tid}/cancel')
def cancel(tid:int,session:DB,user:USER):
    tx,books=lock_tx(session,tid);participant(tx,user)
    if tx.status=='cancelled': return record(tx)
    if tx.status!='accepted': raise HTTPException(409,'Teslimat başladıysa şikâyet/ihtilaf sürecini kullan.')
    cancel_transaction(session,tx,books,user,'Katılımcı iptali')
    return record(tx)
@app.get('/api/me/notifications')
def notifications(session:DB,user:USER, offset:int=Query(0,ge=0), limit:int=Query(100,ge=1,le=200)):
    return [record(n) for n in session.scalars(select(Notification).where(Notification.user_id==user.id).order_by(Notification.id.desc()).offset(offset).limit(limit))]
@app.post('/api/me/notifications/{nid}/read')
def read_notification(nid:int,session:DB,user:USER):
    n=get(session,Notification,nid)
    if n.user_id!=user.id: raise HTTPException(403,'Bu bildirim sana ait değil.')
    n.read_at=now();return record(n)
@app.post('/api/complaints',status_code=201)
def complain(data:ComplaintInput,session:DB,user:USER):
    if data.book_id: get(session,Book,data.book_id)
    if data.transaction_id:
        tx,_=lock_tx(session,data.transaction_id);participant(tx,user)
        if tx.status in ('accepted','in_delivery'): tx.status='disputed'
    if session.scalar(select(Complaint.id).where(Complaint.reporter_id==user.id,Complaint.status=='open',
        Complaint.book_id==data.book_id,Complaint.transaction_id==data.transaction_id)):
        raise HTTPException(409,'Bu kayıt için açık şikâyetin var.')
    row=Complaint(reporter_id=user.id,**data.model_dump());session.add(row);session.flush();audit(session,user,'complaint.created',row)
    return record(row)
@app.get('/api/me/complaints')
def my_complaints(session:DB,user:USER, offset:int=Query(0,ge=0), limit:int=Query(100,ge=1,le=200)):
    return [record(r) for r in session.scalars(select(Complaint).where(Complaint.reporter_id==user.id).order_by(Complaint.id.desc()).offset(offset).limit(limit))]

def impact_data(session):
    return {'mode':'simulation','support_rate':0.2,'completed_transactions':session.scalar(select(func.count()).select_from(Transaction).where(Transaction.status=='completed')),
        'active_books':session.scalar(select(func.count()).select_from(Book).where(Book.status=='active',Book.is_demo==False)),
        'demo_books':session.scalar(select(func.count()).select_from(Book).where(Book.is_demo==True)),
        'simulated_support_cents':session.scalar(select(func.coalesce(func.sum(SupportRecord.amount_cents),0))), 'real_support_cents':0,
        'participants':session.scalar(select(func.count()).select_from(Profile))}
@app.get('/api/impact')
def impact(session:DB): return impact_data(session)
@app.get('/api/support')
def support(session:DB, offset:int=Query(0,ge=0), limit:int=Query(100,ge=1,le=200)):
    # Public report deliberately omits transaction IDs and participant information.
    return [{'id':s.id,'amount_cents':s.amount_cents,'mode':s.mode,'status':s.status,'organization':s.organization,'note':s.note,'created_at':s.created_at} for s in session.scalars(select(SupportRecord).order_by(SupportRecord.id.desc()).offset(offset).limit(limit))]
@app.get('/api/admin/overview')
def overview(session:DB,user:ADMIN):
    return {**impact_data(session),'open_complaints':session.scalar(select(func.count()).select_from(Complaint).where(Complaint.status=='open')),
        'pending_requests':session.scalar(select(func.count()).select_from(Request).where(Request.status=='pending'))}
@app.get('/api/admin/complaints')
def complaints(session:DB,user:ADMIN, offset:int=Query(0,ge=0), limit:int=Query(100,ge=1,le=200)):
    return [record(r) for r in session.scalars(select(Complaint).order_by(Complaint.id.desc()).offset(offset).limit(limit))]
@app.post('/api/admin/complaints/{cid}/resolve')
def resolve(cid:int,data:ResolutionInput,session:DB,user:ADMIN):
    row=get(session,Complaint,cid)
    tx=books=None
    if row.transaction_id: tx,books=lock_tx(session,row.transaction_id)
    session.refresh(row,with_for_update=True)
    if row.status!='open': raise HTTPException(409,'Şikâyet zaten sonuçlanmış.')
    if data.outcome=='cancel_transaction':
        if not tx: raise HTTPException(422,'Bu şikâyet bir işleme ait değil.')
        cancel_transaction(session,tx,books,user,data.resolution)
    row.status='dismissed' if data.outcome=='dismiss' else 'resolved';row.resolution=data.resolution
    session.flush()
    if tx and tx.status=='disputed':
        remaining=session.scalar(select(Complaint.id).where(Complaint.transaction_id==tx.id,Complaint.status=='open').limit(1))
        if remaining is None:
            started=session.scalar(select(Delivery.id).where(Delivery.transaction_id==tx.id,Delivery.status!='pending').limit(1))
            tx.status='in_delivery' if started else 'accepted'
    notify(session,row.reporter_id,'Şikâyetin sonuçlandı. Kararı hesabından inceleyebilirsin.',row.transaction_id)
    audit(session,user,'complaint.resolved',row,data.resolution)
    return record(row)
@app.get('/api/admin/books')
def admin_books(session:DB,user:ADMIN, offset:int=Query(0,ge=0), limit:int=Query(100,ge=1,le=200)):
    return [record(b) for b in session.scalars(select(Book).order_by(Book.id.desc()).offset(offset).limit(limit))]
@app.post('/api/admin/books/{bid}/moderate')
def moderate(bid:int,data:ModerateInput,session:DB,user:ADMIN):
    b=lock_books(session,[bid]).get(bid)
    if not b: raise HTTPException(404,'Kitap bulunamadı.')
    if b.status not in ('active','hidden'): raise HTTPException(409,'İşlemdeki veya arşivli ilan bu ekrandan değiştirilemez.')
    b.status='hidden' if data.hidden else 'active'
    if data.hidden:
        for r in session.scalars(select(Request).where(Request.status=='pending',or_(Request.book_id==bid,Request.offered_book_id==bid))):
            r.status='rejected';notify(session,r.requester_id,'İlan moderasyona alındığı için talebin kapatıldı.')
    if b.owner_id: notify(session,b.owner_id,f'İlan durumu güncellendi: {data.reason}')
    audit(session,user,'book.moderated',b,data.reason);return record(b)
@app.get('/api/admin/support')
def admin_support(session:DB,user:ADMIN, offset:int=Query(0,ge=0), limit:int=Query(100,ge=1,le=200)):
    return [record(s) for s in session.scalars(select(SupportRecord).order_by(SupportRecord.id.desc()).offset(offset).limit(limit))]
@app.patch('/api/admin/support/{sid}')
def update_support(sid:int,data:SupportInput,session:DB,user:ADMIN):
    s=get(session,SupportRecord,sid)
    if data.document_url and not data.document_url.startswith('https://'): raise HTTPException(422,'Belge bağlantısı HTTPS olmalı.')
    for k,v in data.model_dump().items(): setattr(s,k,v)
    s.status='documented';audit(session,user,'support.documented',s,'Simülasyon belgesi');return record(s)
@app.get('/api/admin/audit')
def audit_log(session:DB,user:ADMIN, offset:int=Query(0,ge=0), limit:int=Query(100,ge=1,le=200)):
    return [record(a) for a in session.scalars(select(AuditLog).order_by(AuditLog.id.desc()).offset(offset).limit(limit))]
@app.get('/api/admin/reports.csv')
def report(session:DB,user:ADMIN):
    import io
    out=io.StringIO();writer=csv.writer(out);writer.writerow(['Ölçüt','Değer'])
    for key,value in impact_data(session).items(): writer.writerow([key,value])
    return Response('\ufeff'+out.getvalue(),media_type='text/csv; charset=utf-8',headers={'Content-Disposition':'attachment; filename=patikitap-etki.csv'})

@app.get('/api/admin/users')
def admin_users(session:DB,user:ADMIN, offset:int=Query(0,ge=0), limit:int=Query(100,ge=1,le=200)):
    return [record(p) for p in session.scalars(select(Profile).order_by(Profile.created_at.desc()).offset(offset).limit(limit))]
@app.patch('/api/admin/users/{uid}')
def set_user_state(uid:UUID,data:UserStateInput,session:DB,user:ADMIN):
    if str(uid)==user.id: raise HTTPException(409,'Kendi hesabını bu ekrandan kapatamazsın.')
    profile=get(session,Profile,str(uid));profile.disabled=data.disabled
    audit(session,user,'profile.disabled' if data.disabled else 'profile.enabled',profile,data.reason)
    return record(profile)

@app.get('/api/ready')
def readiness(session:DB):
    for table in Base.metadata.sorted_tables:
        session.execute(select(table).limit(0))
    return {'status':'ready'}

# A built React application can be served from the same origin in production.
DIST=Path(__file__).resolve().parent.parent/'frontend'/'dist'
if DIST.is_dir():
    app.mount('/',StaticFiles(directory=str(DIST),html=True),name='frontend')
