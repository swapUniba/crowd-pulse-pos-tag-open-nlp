crowd-pulse-pos-tag-open-nlp
============================

Multi language Crowd Pulse Part-of-Speech tagger.

----------------------------

The `postagger-opennlp` plugin needs a series of POS tagging models in the class loader accessible 
resources directory, in the form of:

    LANGUAGE-pos-maxent.bin
    
`LANGUAGE` is the two-characters representation of the language of the model.

Here's a list of available POS tagging models:

  - [Italian](https://github.com/aciapetti/opennlp-italian-models/raw/master/models/it/it-pos-maxent.bin) 
  ([source page](https://github.com/aciapetti/opennlp-italian-models))
  - [English](http://opennlp.sourceforge.net/models-1.5/en-pos-maxent.bin) 
  ([source page](http://opennlp.sourceforge.net/models-1.5/))
  - [German](http://opennlp.sourceforge.net/models-1.5/de-pos-maxent.bin) 
  ([source page](http://opennlp.sourceforge.net/models-1.5/))
  - [Danish](http://opennlp.sourceforge.net/models-1.5/da-pos-maxent.bin) 
  ([source page](http://opennlp.sourceforge.net/models-1.5/))
  - [Dutch](http://opennlp.sourceforge.net/models-1.5/nl-pos-maxent.bin) 
  ([source page](http://opennlp.sourceforge.net/models-1.5/))
  - [Portuguese](http://opennlp.sourceforge.net/models-1.5/pt-pos-maxent.bin) 
  ([source page](http://opennlp.sourceforge.net/models-1.5/))
  - [Northern Sami](http://opennlp.sourceforge.net/models-1.5/se-pos-maxent.bin) 
  ([source page](http://opennlp.sourceforge.net/models-1.5/))
