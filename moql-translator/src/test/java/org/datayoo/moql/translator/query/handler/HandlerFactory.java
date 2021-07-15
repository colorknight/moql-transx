package org.datayoo.moql.translator.query.handler;

import org.datayoo.moql.translator.query.MongoQueryConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author BLADE
 * @version 1.0
 * @className HandlerFactory
 * @description TODO
 * @date 7/8/2021 5:57 PM
 **/
public class HandlerFactory {
  private static final Logger logger = LoggerFactory
      .getLogger(HandlerFactory.class);
  /**
   * type : handler
   */
  private static final Map<String, Handler> handlerMap = new HashMap<>(
      HandlerTypeEnum.values().length);

  private boolean initFlag = false;

  private HandlerFactory() {
  }

  private static HandlerFactory handlerFactory = null;

  public static HandlerFactory getInstance() {
    if (null == handlerFactory) {
      synchronized (HandlerFactory.class) {
        if (null == handlerFactory) {
          synchronized (HandlerFactory.class) {
            handlerFactory = new HandlerFactory();
            handlerFactory.init();
          }
        }
      }
    }
    return handlerFactory;
  }

  public void init() {
    HandlerTypeEnum[] handlers = HandlerTypeEnum.values();
    for (int i = 0; i < handlers.length; i++) {
      HandlerTypeEnum handlerE = handlers[i];
      Handler handler = null;
      try {
        handler = (Handler) Class.forName(handlerE.getClazz().getName())
            .newInstance();
      } catch (Exception e) {
        logger.error("handler lode error!", e);
      }
      handlerMap.put(handlerE.getType(), handler);
    }
    initFlag = true;
  }

  public Handler getHandlerByType(String type) throws Exception {
    if (!initFlag) {
      throw new Exception("init handler first");
    }
    Handler handler = handlerMap.get(type);
    if (null == handler) {
      handler = handlerMap.get(MongoQueryConstant.DEFAULT_HANDLER);
    }
    return handler;
  }
}
