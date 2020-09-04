import re
import utils
import json
import hashlib
import logging
import base64
import config
import numpy as np
import sqlalchemy as sa

from datetime import datetime as dt
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base


logging.basicConfig(level=logging.INFO)
Base = declarative_base()

secret = utils.get_secret(config.database['project_id'], config.database['secret_name'])

sacn = 'mysql+pymysql://{}:{}@/{}?unix_socket=/cloudsql/{}:europe-west1:{}'.format(
    config.database['db_user'],
    secret,
    config.database['db_name'],
    config.database['project_id'],
    config.database['instance_id']
)

engine = create_engine(sacn, pool_timeout=config.TIMEOUT)


class ImportKeys(Base):
    __tablename__ = config.ImportKeys
    importId = sa.Column('id', sa.types.INTEGER, nullable=False,
                         primary_key=True, autoincrement=True)
    sourceTag = sa.Column('sourceTag', sa.types.String(128), nullable=False)
    sourceKey = sa.Column('sourceKey', sa.types.String(128), nullable=False)
    delete = sa.Column('delete', sa.types.INTEGER, default=0)
    version = sa.Column('version', sa.types.DATETIME, nullable=False)
    versionEnd = sa.Column('versionEnd', sa.types.DATETIME,
                           default=None, nullable=True)

    def __init__(self, sourceTag, sourceKey, delete, version):
        self.sourceTag = sourceTag
        self.sourceKey = sourceKey
        self.delete = delete
        self.version = version


class ImportMeasureValues(Base):
    __tablename__ = config.ImportMeasureValues
    importId = sa.Column('importId', sa.types.INTEGER,
                         nullable=False, primary_key=True, autoincrement=True)
    sourceId = sa.Column('sourceId', sa.types.INTEGER, nullable=False)
    sourceKey = sa.Column('sourceKey', sa.types.String(128), nullable=False)
    measure = sa.Column('measure', sa.types.String(128), nullable=False)
    value = sa.Column('value', sa.types.TEXT, nullable=True)
    valueDate = sa.Column('valueDate', sa.types.DATETIME, nullable=False)

    def __init__(self, importId, sourceId, sourceKey, measure, value, valueDate):
        self.importId = importId
        self.sourceId = sourceId
        self.sourceKey = sourceKey
        self.delete = measure
        self.version = value
        self.valueDate = valueDate


class Hierarchy(Base):
    __tablename__ = config.Hierarchy
    _id = sa.Column('id', sa.types.INTEGER, nullable=False,
                    primary_key=True, autoincrement=True)
    kind = sa.Column('kind', sa.types.String(128), nullable=False)
    kindKey = sa.Column('kindKey', sa.types.String(128), nullable=True)
    parentKind = sa.Column('parentKind', sa.types.String(128), nullable=False)
    parentKindKey = sa.Column(
        'parentKindKey', sa.types.String(128), nullable=True)
    versionEnd = sa.Column('versionEnd', sa.types.DATETIME, nullable=True)
    created = sa.Column('created', sa.types.TIMESTAMP,
                        nullable=False, default=sa.func.now())
    updated = sa.Column('updated', sa.types.DATETIME,
                        nullable=True, default=None)

    def __init__(self, kind, kindKey, parentKind, parentKindKey, created):
        self.kind = kind
        self.kindKey = kindKey
        self.parentKind = parentKind
        self.parentKindKey = parentKindKey
        self.created = created


def insert_ImportMeasureValues(data_dict, sourceTag, session, sourceKey='sourceKey', valueDate=None, ts=None):
    if ts is None:
        ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")

    sourceKey = data_dict.pop(sourceKey)

    if valueDate is None:
        valueDate = ts

    # Update statement on ImportKeys
    q_update = sa.sql.expression.update(ImportKeys).\
        where(ImportKeys.sourceKey == sourceKey).\
        where(ImportKeys.sourceTag == sourceTag).\
        where(ImportKeys.versionEnd.is_(None)).\
        values(versionEnd=ts)

    session.execute(q_update)
    session.flush()

    # Insert statement into ImportKeys
    keys_insert = ImportKeys(
        sourceKey=sourceKey,
        sourceTag=sourceTag,
        delete=0,
        version=ts,
    )

    session.add(keys_insert)
    session.flush()

    # Create dict containing the data for import
    data = dict(
        importId=keys_insert.importId,
        sourceKey=sourceKey,
        sourceId=0,
        valueDate=valueDate,
    )

    insert_values = []
    for measure, value in data_dict.items():
        if (value != '') and (value is not None):
            insert_values.append(
                {**data, **dict(
                    measure=measure,
                    value=value,
                )
                }
            )

    # Insert data into ImportMeasureValues
    q_insert = sa.sql.expression.insert(
        ImportMeasureValues, values=insert_values)
    session.execute(q_insert)
    session.flush()


def add_sourceKey(j, subscription):
    sourceKey = 'sourceKey'
    # Add sourceKey for different sources:
    if 'cz-mainconnections' in subscription:
        to_hash = str(j['project_id']) + str(j['project_name'])
        j['sourceKey'] = hashlib.sha1(to_hash.encode('utf-8')).hexdigest()
    elif 'cz-consumerconnections' in subscription:
        j['sourceKey'] = j['con_objectid']
    elif 'cz-projects' in subscription:
        j['sourceKey'] = j['ln_id']
    else:
        raise ValueError('Unknown subscription: {}'.format(
            subscription
        ))

    return j, sourceKey


def insert_Hierarchy(data_dict, subscription, session, sourceKey='sourceKey', ts=None):
    if ts is None:
        ts = dt.now().strftime("%Y-%m-%d %H:%M:%S")

    sourceKey = data_dict.get(sourceKey)

    # Regex to find ChangePoint and Connect numbers
    con_opdracht_find = re.compile(r'(100\d{7})')
    con_opdrachtH_find = re.compile(r'(H\d{8})')

    def get_bpnr_regex(cell):
        if isinstance(cell, str):
            reconstruction = re.compile(r'(REC20\d{7})')
            nonreconstruction = re.compile(r'(20\d{7})')
            nonreconstruction2 = re.compile(r'(71\d{7})')
            bpnr = reconstruction.findall(cell if cell != np.nan else '')
            if len(bpnr) == 0:
                bpnr = nonreconstruction.findall(
                    cell if cell != np.nan else '')
            if len(bpnr) == 0:
                bpnr = nonreconstruction2.findall(
                    cell if cell != np.nan else '')
            if len(bpnr) == 0:
                return []
            return bpnr
        else:
            return []

    insert = []
    update_statement = sa.sql.expression.update(Hierarchy).\
        where(Hierarchy.kindKey == sourceKey).\
        where(Hierarchy.versionEnd.is_(None)).\
        values(versionEnd=ts)

    if 'cz-mainconnections' in subscription:
        kinds = set(get_bpnr_regex(data_dict['project_id'])) | set(
            get_bpnr_regex(data_dict['project_name'])) - set([np.nan, 'nan', ''])
        for kind in kinds:
            insert.append(
                dict(
                    kind='cp_id',
                    kindKey=sourceKey,
                    parentKind='cpnr_extracted',
                    parentKindKey=kind,
                    created=ts,
                )
            )

        update_statement = update_statement.\
            where(Hierarchy.kind == 'cp_id').\
            where(Hierarchy.parentKind.in_(['cpnr_extracted']))

    # For consumer connections add opdracht_id and cpnr_extracted to Hierarchy
    elif 'cz-consumerconnections' in subscription:
        insert.append(
            dict(
                kind='con_objectid',
                kindKey=sourceKey,
                parentKind='con_opdrachtid',
                parentKindKey=data_dict['con_opdrachtid'],
                created=ts,
            )
        )

        bpnr = get_bpnr_regex(data_dict['build_plan_no'])
        if len(bpnr) > 0:
            insert.append(
                dict(
                    kind='con_objectid',
                    kindKey=sourceKey,
                    parentKind='cpnr_extracted',
                    parentKindKey=bpnr[0],
                    created=ts,
                )
            )

        update_statement = update_statement.\
            where(Hierarchy.kind == 'con_objectid').\
            where(Hierarchy.parentKind.in_(
                ['cpnr_extracted', 'con_opdrachtid']))

    elif 'cz-projects' in subscription:
        bpnr = ', '.join(list(
            set(get_bpnr_regex(data_dict['search_argument'])) -
            set(['', 'nan', np.nan, None])
        ))
        con_opdrachtid = ', '.join(list(
            set(con_opdrachtH_find.findall(str(data_dict.get('search_argument')) if
                data_dict.get('search_argument', np.nan) is not np.nan else '')) |
            set(con_opdracht_find.findall(str(data_dict.get('search_argument')) if
                data_dict.get('search_argument', np.nan) is not np.nan else '')) -
            set(['', 'nan', np.nan, None])
        ))
        if len(bpnr) > 0:
            insert.append(
                dict(
                    kind='ln_id',
                    kindKey=sourceKey,
                    parentKind='cpnr_extracted',
                    parentKindKey=bpnr,
                    created=ts,
                )
            )
        if len(con_opdrachtid) > 0:
            insert.append(
                dict(
                    kind='ln_id',
                    kindKey=sourceKey,
                    parentKind='con_opdrachtid_extracted',
                    parentKindKey=con_opdrachtid,
                    created=ts,
                )
            )

        update_statement = update_statement.\
            where(Hierarchy.kind == 'ln_id').\
            where(Hierarchy.parentKind.in_(
                ['cpnr_extracted', 'con_opdrachtid_extracted']))

    else:
        raise ValueError('No correct subscription given')

    session.execute(update_statement)

    # Insert values
    if len(insert) > 0:
        q_insert = sa.sql.expression.insert(Hierarchy, values=insert)
        session.execute(q_insert)

    # Flush session
    session.flush()


def send_to_cloudsql(subscription, payload, ts=None):
    records = json.loads(payload.decode())

    session = sa.orm.session.sessionmaker(
        engine, autoflush=False, autocommit=False)()

    try:
        if isinstance(records, dict):
            records = records['subject']

        if isinstance(records, list):
            records = records
        else:
            records = [records]

        count = 0
        for i in records:
            j, sourceKey = add_sourceKey(i, subscription)

            insert_Hierarchy(j, subscription, session,
                             sourceKey=sourceKey, ts=ts)

            insert_ImportMeasureValues(
                j, subscription, session, sourceKey=sourceKey, ts=ts, valueDate=ts)

            count += 1

        session.commit()
        logging.info('Session committed, {} records processed'.format(count))
    except Exception as e:
        session.rollback()
        logging.error('Session rolled back')
        logging.error('Processing failure {}'.format(e))
        raise e
    finally:
        session.close()
        engine.dispose()
        logging.info('Session closed')


def topic_to_cloudsql(request):
    # Verify to Google that the function is within your domain
    if request.method == 'GET':
        return '''
            <html>
                <head>
                    <meta name="google-site-verification" content="{token}" />
                </head>
                <body>
                </body>
            </html>
        '''.format(token=config.SITE_VERIFICATION_CODE)

    try:
        # Extract subscription from subscription string
        envelope = json.loads(request.data.decode('utf-8'))
        payload = base64.b64decode(envelope['message']['data'])
        subscription = envelope['subscription'].split('/')[-1]
    except Exception as e:
        logging.error('Extract of subscription failed')
        logging.debug(e)
        return 'OK', 204

    try:
        # Extract timestamp from string
        ts = envelope['message']['publishTime'].\
            split('.')[0].\
            replace('T', ' ').\
            replace('Z', '')
    except Exception as e:
        logging.error('Extract of publishTime failed')
        logging.debug(e)
        return 'OK', 204

    try:
        send_to_cloudsql(subscription, payload, ts)
    except Exception as e:
        logging.error("Send_to_cloudsql function failed")
        logging.debug(e)
        return 'OK', 204

    # Returning any 2xx status indicates successful receipt of the message.
    # 204: no content, delivery successfull, no further actions needed
    return 'OK', 204
