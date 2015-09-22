/**
 * Copyright 2015 Marco Scavuzzo
 * Contact: Marco Scavuzzo <marco.scavuzzo@polimi.it>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * 
 */
package it.polimi.hegira.utils;

import com.beust.jcommander.Parameter;

/**
 * Defines the parameters needed to launch the program.
 * @author Marco Scavuzzo
 */
public class CLI {
	
	@Parameter(names = { "--type", "-t" }, description = "launch a SRC or a TWC",
			required = true, validateWith = ComponentValidator.class)
	public String componentType;
	
	@Parameter(names = { "--queue","-q" }, description = "RabbitMQ queue address.")
	public String queueAddress = "localhost";
}
