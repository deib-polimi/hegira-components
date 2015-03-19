/**
 * Interface to be implemented by any class which aims at migrating from one DB to another
 */
package it.polimi.hegira.transformers;

import it.polimi.hegira.models.Metamodel;


/**
 * Interface defining the methods to perform the transformations
 * from and to the intermediate meta-model.
 * @author Marco Scavuzzo
 *
 */
public interface ITransformer<DBModel> {
	public Metamodel toMyModel(DBModel model);
	public DBModel fromMyModel(Metamodel model);
}
