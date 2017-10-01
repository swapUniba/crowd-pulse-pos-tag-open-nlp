package com.github.frapontillo.pulse.crowd.postag.opennlp;

import com.github.frapontillo.pulse.crowd.data.entity.Message;
import com.github.frapontillo.pulse.crowd.data.entity.Token;
import com.github.frapontillo.pulse.crowd.postag.IPOSTaggerOperator;
import com.github.frapontillo.pulse.spi.IPlugin;
import com.github.frapontillo.pulse.spi.VoidConfig;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import rx.Observable;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Francesco Pontillo
 */
public class OpenNLPPOSTagger extends IPlugin<Message, Message, VoidConfig> {
    public final static String PLUGIN_NAME = "postagger-opennlp";
    private Map<String, POSModel> models;

    public OpenNLPPOSTagger() {
        models = new HashMap<>();
    }

    @Override public String getName() {
        return PLUGIN_NAME;
    }

    @Override public VoidConfig getNewParameter() {
        return new VoidConfig();
    }

    @Override public Observable.Operator<Message, Message> getOperator(VoidConfig parameters) {
        return new IPOSTaggerOperator(this) {
            @Override public List<Token> posTagMessageTokens(Message message) {
                if (message.getTokens() == null) {
                    return null;
                }
                POSModel posModel = getModel(message.getLanguage());
                if (posModel == null) {
                    return null;
                }
                POSTaggerME posTagger = new POSTaggerME(posModel);

                // transform the List of Tokens to an Array of Strings
                List<String> posTagsList = message.getTokens().stream().map(Token::getText)
                        .collect(Collectors.toList());
                String[] posTags = posTagsList.toArray(new String[posTagsList.size()]);
                // fire up the POS-tagging, get the Token POS tags
                posTags = posTagger.tag(posTags);

                // associate each POS with the corresponding Token
                for (int i = 0; i < message.getTokens().size(); i++) {
                    message.getTokens().get(i).setPos(posTags[i]);
                }

                return message.getTokens();
            }
        };
    }

    private POSModel getModel(String language) {
        POSModel model;
        if ((model = models.get(language)) == null) {
            InputStream modelIn = null;
            try {
                modelIn = getClass().getClassLoader()
                        .getResourceAsStream(language + "-pos-maxent.bin");
                model = new POSModel(modelIn);
                models.put(language, model);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (IllegalArgumentException ignored) {
            } finally {
                if (modelIn != null) {
                    try {
                        modelIn.close();
                    } catch (IOException ignored) {
                    }
                }
            }
        }
        return model;
    }
}
