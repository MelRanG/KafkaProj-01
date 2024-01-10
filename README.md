# Java 기반 Producer 구현 실습 및 Producer 내부 매커니즘 이해-01
## Java Producer 구현
- CLI와 마찬가지로 props에 설정을 추가한 뒤 ProducerRecord에 메시지를 추가하고 flush를 한 뒤 send로 보내면 Consumer에서 읽을 수 있다.
- RecoredMetadata로 동기방식 전송인 send().get()을 받는 방법 학습
- 같은 방법으로 콜백을 사용해서 비동기로 받는 방법 습득, 콜백을 보내면 send안에 network 쓰레드가 인자 값을 담아서 리턴해준다.
- 파티션 3개짜리 토픽을 만든 후 프로듀서에서 while문으로 메시지를 무한대로 보냈다. 컨수머가 1개에서 3개로 늘어남에따라 리밸런싱 작업을 하면서 컨수머에게 파티션이 분배된다.
- 이말인 즉 파티션 개수보다 컨수머가 많아봤자 의미없다.