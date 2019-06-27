package com.agentbatch.job;
import org.springframework.batch.item.ItemProcessor;

import com.agentbatch.job.MongoDBEntity;

public class MongoDBProcessor implements
		ItemProcessor<MongoDBEntity, MongoDBEntity> {

	@Override
	public MongoDBEntity process(MongoDBEntity entity) throws Exception {
		return entity;
	}

}