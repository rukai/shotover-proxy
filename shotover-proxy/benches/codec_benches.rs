use bytes::BytesMut;
use cassandra_protocol::frame::message_result::{
    ColSpec, ColType, ColTypeOption, ColTypeOptionValue, RowsMetadata, RowsMetadataFlags, TableSpec,
};
use cassandra_protocol::{frame::Version, query::QueryParams};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use shotover_proxy::codec::cassandra::CassandraCodec;
use shotover_proxy::frame::cassandra::parse_statement_single;
use shotover_proxy::frame::{CassandraFrame, CassandraOperation, CassandraResult, Frame};
use shotover_proxy::message::{IntSize, Message, MessageValue};
use tokio_util::codec::Encoder;

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec");
    group.noise_threshold(0.2);

    {
        let messages = vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 0,
            tracing_id: None,
            warnings: vec![],
            operation: CassandraOperation::Query {
                query: Box::new(parse_statement_single("SELECT * FROM system.local;")),
                params: Box::new(QueryParams::default()),
            },
        }))];

        let mut codec = CassandraCodec::new();

        group.bench_function("encode_cassandra_system.local_query", |b| {
            b.iter_batched(
                || messages.clone(),
                |messages| {
                    let mut bytes = BytesMut::new();
                    codec.encode(messages, &mut bytes).unwrap();
                    black_box(bytes)
                },
                BatchSize::SmallInput,
            )
        });
    }

    {
        let messages = vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 0,
            tracing_id: None,
            warnings: vec![],
            operation: CassandraOperation::Result(peers_v2_result()),
        }))];

        let mut codec = CassandraCodec::new();

        group.bench_function("encode_cassandra_system.local_result", |b| {
            b.iter_batched(
                || messages.clone(),
                |messages| {
                    let mut bytes = BytesMut::new();
                    codec.encode(messages, &mut bytes).unwrap();
                    black_box(bytes)
                },
                BatchSize::SmallInput,
            )
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

fn peers_v2_result() -> CassandraResult {
    CassandraResult::Rows {
        rows: vec![vec![
            MessageValue::Varchar("local".into()),
            MessageValue::Varchar("COMPLETED".into()),
            MessageValue::Inet("127.0.0.1".parse().unwrap()),
            MessageValue::Integer(7000, IntSize::I32),
            MessageValue::Varchar("TestCluster".into()),
            MessageValue::Varchar("3.4.5".into()),
            MessageValue::Varchar("dc1".into()),
            MessageValue::Integer(1662429909, IntSize::I32),
            MessageValue::Uuid("2dd022d6-2937-4754-89d6-02d2933a8f7a".parse().unwrap()),
            MessageValue::Inet("127.0.0.1".parse().unwrap()),
            MessageValue::Integer(7000, IntSize::I32),
            MessageValue::Varchar("5".into()),
            MessageValue::Varchar("org.apache.cassandra.dht.Murmur3Partitioner".into()),
            MessageValue::Varchar("rack1".into()),
            MessageValue::Varchar("4.0.6".into()),
            MessageValue::Inet("0.0.0.0".parse().unwrap()),
            MessageValue::Integer(9042, IntSize::I32),
            MessageValue::Uuid("397ea5ef-1dfa-4760-827c-71b130deeff1".parse().unwrap()),
            MessageValue::List(vec![
                MessageValue::Varchar("-1053286887251221439".into()),
                MessageValue::Varchar("-106570420317937252".into()),
                MessageValue::Varchar("-1118500511339798278".into()),
                MessageValue::Varchar("-1169241438815364676".into()),
                MessageValue::Varchar("-1233774346411700013".into()),
                MessageValue::Varchar("-1283007285177402315".into()),
                MessageValue::Varchar("-1326494175141678507".into()),
                MessageValue::Varchar("-1391881896845523737".into()),
                MessageValue::Varchar("-1407090101949198040".into()),
                MessageValue::Varchar("-1453855538082496644".into()),
                MessageValue::Varchar("-1515967539091591462".into()),
                MessageValue::Varchar("-1533489865785121542".into()),
                MessageValue::Varchar("-1576157525159793283".into()),
                MessageValue::Varchar("-162157776874719171".into()),
                MessageValue::Varchar("-1643837294794869601".into()),
                MessageValue::Varchar("-1680164836710734319".into()),
                MessageValue::Varchar("-1732769176205285547".into()),
                MessageValue::Varchar("-1802660587057735092".into()),
                MessageValue::Varchar("-1841456610901604190".into()),
                MessageValue::Varchar("-1879340784789368908".into()),
                MessageValue::Varchar("-1930662097533829005".into()),
                MessageValue::Varchar("-193234807999467820".into()),
                MessageValue::Varchar("-1952312776363345492".into()),
                MessageValue::Varchar("-2010595959477605104".into()),
                MessageValue::Varchar("-2047162668796828666".into()),
                MessageValue::Varchar("-2103524200192330661".into()),
                MessageValue::Varchar("-2136351916082225294".into()),
                MessageValue::Varchar("-2211818113880839641".into()),
                MessageValue::Varchar("-2253026203848089498".into()),
                MessageValue::Varchar("-2306232275618633861".into()),
                MessageValue::Varchar("-2366762723148986512".into()),
                MessageValue::Varchar("-2398213000465113021".into()),
                MessageValue::Varchar("-2451513595470736998".into()),
                MessageValue::Varchar("-245424348530919796".into()),
                MessageValue::Varchar("-2485765597152082750".into()),
                MessageValue::Varchar("-2541024644192793504".into()),
                MessageValue::Varchar("-2548126194762290937".into()),
                MessageValue::Varchar("-2584465729882569296".into()),
                MessageValue::Varchar("-2629893092487733861".into()),
                MessageValue::Varchar("-2653555520877023374".into()),
                MessageValue::Varchar("-2695950381051029246".into()),
                MessageValue::Varchar("-2759537644158335976".into()),
                MessageValue::Varchar("-2800328130355888275".into()),
                MessageValue::Varchar("-2843129825641154811".into()),
                MessageValue::Varchar("-2904470691585836218".into()),
                MessageValue::Varchar("-2967240337412998128".into()),
                MessageValue::Varchar("-300828578727447484".into()),
                MessageValue::Varchar("-3009522390354666974".into()),
                MessageValue::Varchar("-3069475490760330304".into()),
                MessageValue::Varchar("-3088401570202593613".into()),
                MessageValue::Varchar("-3138589667889054545".into()),
                MessageValue::Varchar("-3207852532422258106".into()),
                MessageValue::Varchar("-3240157743632548808".into()),
                MessageValue::Varchar("-3301648437477540995".into()),
                MessageValue::Varchar("-3354106246635327873".into()),
                MessageValue::Varchar("-3388016705890739503".into()),
                MessageValue::Varchar("-3444922072378403206".into()),
                MessageValue::Varchar("-3495453336869229616".into()),
                MessageValue::Varchar("-353168487411060067".into()),
                MessageValue::Varchar("-3532270068745407705".into()),
                MessageValue::Varchar("-3580206458269889837".into()),
                MessageValue::Varchar("-3658562915663499950".into()),
                MessageValue::Varchar("-3666640389151057780".into()),
                MessageValue::Varchar("-3700619198255472071".into()),
                MessageValue::Varchar("-3754049964760558007".into()),
                MessageValue::Varchar("-3807585812801555352".into()),
                MessageValue::Varchar("-3848675294420850973".into()),
                MessageValue::Varchar("-385440080170531058".into()),
                MessageValue::Varchar("-3910605082546569993".into()),
                MessageValue::Varchar("-3955281892282785998".into()),
                MessageValue::Varchar("-4021126431389330202".into()),
                MessageValue::Varchar("-4072929672183074707".into()),
                MessageValue::Varchar("-4138408827936160334".into()),
                MessageValue::Varchar("-4157900636303623518".into()),
                MessageValue::Varchar("-4223696570626851441".into()),
                MessageValue::Varchar("-4262936741388560065".into()),
                MessageValue::Varchar("-4329054442683003056".into()),
                MessageValue::Varchar("-4330220362242958762".into()),
                MessageValue::Varchar("-436644831030142346".into()),
                MessageValue::Varchar("-4370056871164132065".into()),
                MessageValue::Varchar("-4432538416242080842".into()),
                MessageValue::Varchar("-4452116165357195997".into()),
                MessageValue::Varchar("-447014253779697377".into()),
                MessageValue::Varchar("-4505128235357924458".into()),
                MessageValue::Varchar("-4571209218094809156".into()),
                MessageValue::Varchar("-4648120762324841346".into()),
                MessageValue::Varchar("-4717394893147059049".into()),
                MessageValue::Varchar("-4795241631068571608".into()),
                MessageValue::Varchar("-4847135636540662810".into()),
                MessageValue::Varchar("-4928100176324910967".into()),
                MessageValue::Varchar("-4979991867611160324".into()),
                MessageValue::Varchar("-5041141770579346068".into()),
                MessageValue::Varchar("-5120544580329844275".into()),
                MessageValue::Varchar("-5179090870774393369".into()),
                MessageValue::Varchar("-5230350850137708564".into()),
                MessageValue::Varchar("-527273302598393468".into()),
                MessageValue::Varchar("-5306754816121976370".into()),
                MessageValue::Varchar("-5343341207081001140".into()),
                MessageValue::Varchar("-5388881150707159950".into()),
                MessageValue::Varchar("-5428874231606218352".into()),
                MessageValue::Varchar("-5492497409816003103".into()),
                MessageValue::Varchar("-5553387924544763987".into()),
                MessageValue::Varchar("-5593809836937936630".into()),
                MessageValue::Varchar("-5651628658300363087".into()),
                MessageValue::Varchar("-5662262536212885409".into()),
                MessageValue::Varchar("-5707251483611981081".into()),
                MessageValue::Varchar("-5764048300972564294".into()),
                MessageValue::Varchar("-578583790895217637".into()),
                MessageValue::Varchar("-5803870505152483214".into()),
                MessageValue::Varchar("-5845946725096068838".into()),
                MessageValue::Varchar("-58673803315641386".into()),
                MessageValue::Varchar("-5914217934162231273".into()),
                MessageValue::Varchar("-5919462482734346233".into()),
                MessageValue::Varchar("-5961825585269426164".into()),
                MessageValue::Varchar("-6019810822063969717".into()),
                MessageValue::Varchar("-6073041226425096764".into()),
                MessageValue::Varchar("-6112331883326273396".into()),
                MessageValue::Varchar("-6173743445282830250".into()),
                MessageValue::Varchar("-6201042736901190677".into()),
                MessageValue::Varchar("-6251976154389366291".into()),
                MessageValue::Varchar("-6330896149498434702".into()),
                MessageValue::Varchar("-6406732555449586966".into()),
                MessageValue::Varchar("-6432477073634185852".into()),
                MessageValue::Varchar("-6491545169337203540".into()),
                MessageValue::Varchar("-652453200366894428".into()),
                MessageValue::Varchar("-6529330420810611401".into()),
                MessageValue::Varchar("-6576612914985995533".into()),
                MessageValue::Varchar("-657719536957549295".into()),
                MessageValue::Varchar("-6583794520508215363".into()),
                MessageValue::Varchar("-6618106006869208679".into()),
                MessageValue::Varchar("-6668593639832474693".into()),
                MessageValue::Varchar("-6701772576942435295".into()),
                MessageValue::Varchar("-6756268410124392265".into()),
                MessageValue::Varchar("-6818506834129652609".into()),
                MessageValue::Varchar("-6861511773295862196".into()),
                MessageValue::Varchar("-6923936160244385046".into()),
                MessageValue::Varchar("-6965815207939944022".into()),
                MessageValue::Varchar("-702143626612579554".into()),
                MessageValue::Varchar("-7033907288367203523".into()),
                MessageValue::Varchar("-7070708769723249947".into()),
                MessageValue::Varchar("-7146398783089308371".into()),
                MessageValue::Varchar("-7184575685466032188".into()),
                MessageValue::Varchar("-7237620976780359800".into()),
                MessageValue::Varchar("-7314104561578788957".into()),
                MessageValue::Varchar("-7339856130127691976".into()),
                MessageValue::Varchar("-7387341485156361661".into()),
                MessageValue::Varchar("-7451004336948210538".into()),
                MessageValue::Varchar("-7478233171789619778".into()),
                MessageValue::Varchar("-7540633815441714500".into()),
                MessageValue::Varchar("-7572029076844902667".into()),
                MessageValue::Varchar("-761803330890584664".into()),
                MessageValue::Varchar("-7618654367104839633".into()),
                MessageValue::Varchar("-7679836420743153652".into()),
                MessageValue::Varchar("-7715302711745764878".into()),
                MessageValue::Varchar("-7787172699037781843".into()),
                MessageValue::Varchar("-7850587097637251509".into()),
                MessageValue::Varchar("-7884679363341611299".into()),
                MessageValue::Varchar("-7937021028518419452".into()),
                MessageValue::Varchar("-7961083598894068837".into()),
                MessageValue::Varchar("-798559017799826755".into()),
                MessageValue::Varchar("-8012772684469989757".into()),
                MessageValue::Varchar("-8077966452168917024".into()),
                MessageValue::Varchar("-8131423367483532508".into()),
                MessageValue::Varchar("-8166946513735346444".into()),
                MessageValue::Varchar("-8225662531650147670".into()),
                MessageValue::Varchar("-8271684543548777051".into()),
                MessageValue::Varchar("-8346605655512010775".into()),
                MessageValue::Varchar("-8408789467303522006".into()),
                MessageValue::Varchar("-8451716476555525946".into()),
                MessageValue::Varchar("-850163940962482603".into()),
                MessageValue::Varchar("-8507017377751998651".into()),
                MessageValue::Varchar("-8548168288992657798".into()),
                MessageValue::Varchar("-8600601001610320434".into()),
                MessageValue::Varchar("-8606494085783673520".into()),
                MessageValue::Varchar("-8657080191709078624".into()),
                MessageValue::Varchar("-8722496804724557669".into()),
                MessageValue::Varchar("-8761230575244592800".into()),
                MessageValue::Varchar("-8816463672026944964".into()),
                MessageValue::Varchar("-8886426330656473835".into()),
                MessageValue::Varchar("-8918501401692203018".into()),
                MessageValue::Varchar("-8976273572946855876".into()),
                MessageValue::Varchar("-9054825905108122129".into()),
                MessageValue::Varchar("-9065622270435933282".into()),
                MessageValue::Varchar("-9124209862469428289".into()),
                MessageValue::Varchar("-9154952761677859578".into()),
                MessageValue::Varchar("-920361893982479193".into()),
                MessageValue::Varchar("-9205998296664244522".into()),
                MessageValue::Varchar("-959970210770346892".into()),
                MessageValue::Varchar("-999975303519652468".into()),
                MessageValue::Varchar("1030222895734812861".into()),
                MessageValue::Varchar("1063731083911270551".into()),
                MessageValue::Varchar("107818616681201012".into()),
                MessageValue::Varchar("112480003063738152".into()),
                MessageValue::Varchar("1136290717253665518".into()),
                MessageValue::Varchar("1181979069164768056".into()),
                MessageValue::Varchar("1222859986767344417".into()),
                MessageValue::Varchar("1282883050143318175".into()),
                MessageValue::Varchar("1326500357976020626".into()),
                MessageValue::Varchar("1365909947781525452".into()),
                MessageValue::Varchar("1434831827110874202".into()),
                MessageValue::Varchar("1484201229063580712".into()),
                MessageValue::Varchar("1510842995209025695".into()),
                MessageValue::Varchar("1571834839178975815".into()),
                MessageValue::Varchar("1614889509643212856".into()),
                MessageValue::Varchar("163265740678521809".into()),
                MessageValue::Varchar("1640487546879627807".into()),
                MessageValue::Varchar("1705297175215364486".into()),
                MessageValue::Varchar("1729355995174568165".into()),
                MessageValue::Varchar("1800360739872152532".into()),
                MessageValue::Varchar("1846448981948191415".into()),
                MessageValue::Varchar("1861776679721810".into()),
                MessageValue::Varchar("1903617916218375157".into()),
                MessageValue::Varchar("193489307933790068".into()),
                MessageValue::Varchar("1959361367861888547".into()),
                MessageValue::Varchar("1998979663237001201".into()),
                MessageValue::Varchar("2058562525486522027".into()),
                MessageValue::Varchar("2120723964562291790".into()),
                MessageValue::Varchar("2166856439175031009".into()),
                MessageValue::Varchar("2215192542268176877".into()),
                MessageValue::Varchar("2251250724552462384".into()),
                MessageValue::Varchar("2318067863004016178".into()),
                MessageValue::Varchar("2319225229995541919".into()),
                MessageValue::Varchar("2381846349184783573".into()),
                MessageValue::Varchar("2428924028465757481".into()),
                MessageValue::Varchar("2467783999193743362".into()),
                MessageValue::Varchar("247725071650470321".into()),
                MessageValue::Varchar("249254207978031467".into()),
                MessageValue::Varchar("2535823555480865323".into()),
                MessageValue::Varchar("2590215802656627353".into()),
                MessageValue::Varchar("2599741064624157916".into()),
                MessageValue::Varchar("2649739639682596335".into()),
                MessageValue::Varchar("2689083302891684496".into()),
                MessageValue::Varchar("2754413100275770208".into()),
                MessageValue::Varchar("2817679391525171433".into()),
                MessageValue::Varchar("2863290537418163630".into()),
                MessageValue::Varchar("2884512583568294858".into()),
                MessageValue::Varchar("2931722548687614195".into()),
                MessageValue::Varchar("2969093904822452449".into()),
                MessageValue::Varchar("3028385995134245917".into()),
                MessageValue::Varchar("3036606292955661657".into()),
                MessageValue::Varchar("304050136121377187".into()),
                MessageValue::Varchar("3110373498393863835".into()),
                MessageValue::Varchar("3161614849707931198".into()),
                MessageValue::Varchar("3217093752116140230".into()),
                MessageValue::Varchar("3258382241972158580".into()),
                MessageValue::Varchar("3314163569464468106".into()),
                MessageValue::Varchar("3350018745384882477".into()),
                MessageValue::Varchar("3425431416062890618".into()),
                MessageValue::Varchar("346993658581393029".into()),
                MessageValue::Varchar("3472333282539228366".into()),
                MessageValue::Varchar("3508577308476777005".into()),
                MessageValue::Varchar("3561734717669202147".into()),
                MessageValue::Varchar("359775556820791677".into()),
                MessageValue::Varchar("3599267619603735560".into()),
                MessageValue::Varchar("3617927439000467242".into()),
                MessageValue::Varchar("3691874056121208030".into()),
                MessageValue::Varchar("3739747260259335087".into()),
                MessageValue::Varchar("3792467575625853618".into()),
                MessageValue::Varchar("3833735808337219325".into()),
                MessageValue::Varchar("3901850305506109551".into()),
                MessageValue::Varchar("3947442046033373244".into()),
                MessageValue::Varchar("4024956290836441874".into()),
                MessageValue::Varchar("4077134487564787399".into()),
                MessageValue::Varchar("411297405619220081".into()),
                MessageValue::Varchar("4113992687741467801".into()),
                MessageValue::Varchar("41368531426448077".into()),
                MessageValue::Varchar("4163810219049424418".into()),
                MessageValue::Varchar("4216948767723528746".into()),
                MessageValue::Varchar("4249601410873011666".into()),
                MessageValue::Varchar("4306544265635989688".into()),
                MessageValue::Varchar("4346428801926007609".into()),
                MessageValue::Varchar("4378199256048562683".into()),
                MessageValue::Varchar("4432054265677051785".into()),
                MessageValue::Varchar("4472506620499555205".into()),
                MessageValue::Varchar("449683977510099902".into()),
                MessageValue::Varchar("4518105711017831993".into()),
                MessageValue::Varchar("4565243953134324620".into()),
                MessageValue::Varchar("4617374297948754701".into()),
                MessageValue::Varchar("4674073777645752410".into()),
                MessageValue::Varchar("4717892712775449163".into()),
                MessageValue::Varchar("4765114531994624473".into()),
                MessageValue::Varchar("4801100051415754824".into()),
                MessageValue::Varchar("4861907410776146213".into()),
                MessageValue::Varchar("4926306170851979261".into()),
                MessageValue::Varchar("4970365137677945421".into()),
                MessageValue::Varchar("4989527803867045424".into()),
                MessageValue::Varchar("5059864525358175145".into()),
                MessageValue::Varchar("5104185976087488498".into()),
                MessageValue::Varchar("5121537529234067814".into()),
                MessageValue::Varchar("516330674606803661".into()),
                MessageValue::Varchar("5194840708074843155".into()),
                MessageValue::Varchar("5242869897471595252".into()),
                MessageValue::Varchar("5289937103685716107".into()),
                MessageValue::Varchar("530719412048393152".into()),
                MessageValue::Varchar("5334111723278632223".into()),
                MessageValue::Varchar("5382392968492049698".into()),
                MessageValue::Varchar("5415132859097597143".into()),
                MessageValue::Varchar("5493240626134706089".into()),
                MessageValue::Varchar("5569869348558516421".into()),
                MessageValue::Varchar("5596880997343382298".into()),
                MessageValue::Varchar("5669346056590191291".into()),
                MessageValue::Varchar("5720532317166890482".into()),
                MessageValue::Varchar("5754581868430942384".into()),
                MessageValue::Varchar("579792355629466266".into()),
                MessageValue::Varchar("5829757778830179168".into()),
                MessageValue::Varchar("5885270149010574528".into()),
                MessageValue::Varchar("5935308853601875072".into()),
                MessageValue::Varchar("5975677814582726158".into()),
                MessageValue::Varchar("6044940066858657433".into()),
                MessageValue::Varchar("6116829621315553087".into()),
                MessageValue::Varchar("611817723703861719".into()),
                MessageValue::Varchar("6172113745083642653".into()),
                MessageValue::Varchar("6215279835448121105".into()),
                MessageValue::Varchar("6269360302604362873".into()),
                MessageValue::Varchar("6343801543522170786".into()),
                MessageValue::Varchar("6389190735304408098".into()),
                MessageValue::Varchar("6391104603929653462".into()),
                MessageValue::Varchar("6453087171424913833".into()),
                MessageValue::Varchar("6496527013599036291".into()),
                MessageValue::Varchar("6554245173561108665".into()),
                MessageValue::Varchar("6589605869362903591".into()),
                MessageValue::Varchar("6635729618900475440".into()),
                MessageValue::Varchar("6688822536995061684".into()),
                MessageValue::Varchar("6725359097393979403".into()),
                MessageValue::Varchar("6738164638561105034".into()),
                MessageValue::Varchar("676663160461110210".into()),
                MessageValue::Varchar("6812209884287209063".into()),
                MessageValue::Varchar("6862258872763400985".into()),
                MessageValue::Varchar("6870121703991519588".into()),
                MessageValue::Varchar("6919993169761775437".into()),
                MessageValue::Varchar("6959463942259046168".into()),
                MessageValue::Varchar("7029964651252881570".into()),
                MessageValue::Varchar("7093736484814701305".into()),
                MessageValue::Varchar("7142456145974986419".into()),
                MessageValue::Varchar("7154893222935656530".into()),
                MessageValue::Varchar("719147164499683752".into()),
                MessageValue::Varchar("7234510929056080953".into()),
                MessageValue::Varchar("7298766634501607589".into()),
                MessageValue::Varchar("7314851661046327745".into()),
                MessageValue::Varchar("7380554294986428671".into()),
                MessageValue::Varchar("7420095024217797677".into()),
                MessageValue::Varchar("7431995489075292870".into()),
                MessageValue::Varchar("7487018446356109884".into()),
                MessageValue::Varchar("7528762881339520252".into()),
                MessageValue::Varchar("7592568913833974580".into()),
                MessageValue::Varchar("7653965050391528958".into()),
                MessageValue::Varchar("7695812055430252290".into()),
                MessageValue::Varchar("7743886360708004089".into()),
                MessageValue::Varchar("774927302498132054".into()),
                MessageValue::Varchar("7806153317721812930".into()),
                MessageValue::Varchar("7845467284843755239".into()),
                MessageValue::Varchar("7869648258971097232".into()),
                MessageValue::Varchar("7948492852229030510".into()),
                MessageValue::Varchar("8002619989059359691".into()),
                MessageValue::Varchar("8010127899626696759".into()),
                MessageValue::Varchar("8058518190900093487".into()),
                MessageValue::Varchar("8094201397447483830".into()),
                MessageValue::Varchar("8156552612278220223".into()),
                MessageValue::Varchar("8217822685400734916".into()),
                MessageValue::Varchar("8256900951607843708".into()),
                MessageValue::Varchar("8334144973840653479".into()),
                MessageValue::Varchar("833805336720126826".into()),
                MessageValue::Varchar("8384373327108829473".into()),
                MessageValue::Varchar("8412315133369625646".into()),
                MessageValue::Varchar("8473863667935968391".into()),
                MessageValue::Varchar("8519982050240373338".into()),
                MessageValue::Varchar("8524734776041826854".into()),
                MessageValue::Varchar("8579659334710038418".into()),
                MessageValue::Varchar("8616809441293369281".into()),
                MessageValue::Varchar("8683866024526186839".into()),
                MessageValue::Varchar("8742887259866916877".into()),
                MessageValue::Varchar("8795515464445976787".into()),
                MessageValue::Varchar("882462547635745377".into()),
                MessageValue::Varchar("8835624592501686292".into()),
                MessageValue::Varchar("8869608618220213571".into()),
                MessageValue::Varchar("8940371677333861754".into()),
                MessageValue::Varchar("8988273352142810835".into()),
                MessageValue::Varchar("8997272563567796572".into()),
                MessageValue::Varchar("9046467394465522107".into()),
                MessageValue::Varchar("9081239282957691906".into()),
                MessageValue::Varchar("9132288050143507885".into()),
                MessageValue::Varchar("916274392732033795".into()),
                MessageValue::Varchar("9196371566731029617".into()),
                MessageValue::Varchar("972489258104233580".into()),
            ]),
            MessageValue::Null,
        ]],
        metadata: Box::new(RowsMetadata {
            flags: RowsMetadataFlags::GLOBAL_TABLE_SPACE,
            columns_count: 40,
            paging_state: None,
            new_metadata_id: None,
            global_table_spec: Some(TableSpec {
                ks_name: "system".into(),
                table_name: "local".into(),
            }),
            col_specs: vec![
                ColSpec {
                    table_spec: None,
                    name: "key".into(),
                    col_type: ColTypeOption {
                        id: ColType::Varchar,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "bootstrapped".into(),
                    col_type: ColTypeOption {
                        id: ColType::Varchar,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "broadcast_address".into(),
                    col_type: ColTypeOption {
                        id: ColType::Inet,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "broadcast_port".into(),
                    col_type: ColTypeOption {
                        id: ColType::Int,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "cluster_name".into(),
                    col_type: ColTypeOption {
                        id: ColType::Varchar,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "cql_version".into(),
                    col_type: ColTypeOption {
                        id: ColType::Varchar,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "data_center".into(),
                    col_type: ColTypeOption {
                        id: ColType::Varchar,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "gossip_generation".into(),
                    col_type: ColTypeOption {
                        id: ColType::Int,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "host_id".into(),
                    col_type: ColTypeOption {
                        id: ColType::Uuid,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "listen_address".into(),
                    col_type: ColTypeOption {
                        id: ColType::Inet,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "listen_port".into(),
                    col_type: ColTypeOption {
                        id: ColType::Int,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "native_protocol_version".into(),
                    col_type: ColTypeOption {
                        id: ColType::Varchar,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "partitioner".into(),
                    col_type: ColTypeOption {
                        id: ColType::Varchar,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "rack".into(),
                    col_type: ColTypeOption {
                        id: ColType::Varchar,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "release_version".into(),
                    col_type: ColTypeOption {
                        id: ColType::Varchar,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "rpc_address".into(),
                    col_type: ColTypeOption {
                        id: ColType::Inet,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "rpc_port".into(),
                    col_type: ColTypeOption {
                        id: ColType::Int,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "schema_version".into(),
                    col_type: ColTypeOption {
                        id: ColType::Uuid,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "tokens".into(),
                    col_type: ColTypeOption {
                        id: ColType::Set,
                        value: Some(ColTypeOptionValue::CSet(Box::new(ColTypeOption {
                            id: ColType::Varchar,
                            value: None,
                        }))),
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "truncated_at".into(),
                    col_type: ColTypeOption {
                        id: ColType::Map,
                        value: Some(ColTypeOptionValue::CMap(
                            Box::new(ColTypeOption {
                                id: ColType::Uuid,
                                value: None,
                            }),
                            Box::new(ColTypeOption {
                                id: ColType::Blob,
                                value: None,
                            }),
                        )),
                    },
                },
            ],
        }),
    }
}
