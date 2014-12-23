/**
 * Interface to be implemented by any class which aims at migrating from one DB to another
 */
package it.polimi.hegira.transformers;

import it.polimi.hegira.models.Metamodel;


/**
 * @author Marco Scavuzzo
 *
 */
public interface ITransformer<DBModel> {
	public Metamodel toMyModel(DBModel model);
	public DBModel fromMyModel(Metamodel model);
}
